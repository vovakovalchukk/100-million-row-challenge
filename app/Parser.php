<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int WORKER_COUNT = 2;
    private const int WRITE_BUFFER = 8 * 1024 * 1024;

    private const int DOMAIN_LEN = 19;
    private const int TS_LEN     = 25;
    private const int DATE_LEN   = 10;

    private bool $hasIgbinary;
    private int  $readChunk;

    public function __construct()
    {
        $this->hasIgbinary = function_exists('igbinary_serialize');

        $this->readChunk = PHP_OS_FAMILY === 'Linux'
            ? 32 * 1024 * 1024
            :  4 * 1024 * 1024;
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '-1');

        $chunks = $this->getChunks($inputPath);
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $pid    = getmypid();

        $tempFiles = [];
        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $tempFiles[$i] = "{$tmpDir}/php100m_{$pid}_{$i}.bin";
        }

        $pids = [];
        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            $fork = pcntl_fork();

            if ($fork === -1) {
                throw new \RuntimeException('pcntl_fork failed');
            }

            if ($fork === 0) {
                ini_set('memory_limit', '-1');
                $this->runWorker(
                    $inputPath,
                    $chunks[$i][0],
                    $chunks[$i][1],
                    $tempFiles[$i],
                );
                exit(0);
            }

            $pids[] = $fork;
        }

        foreach ($pids as $fork) {
            pcntl_waitpid($fork, $status);

            if (!pcntl_wifexited($status) || pcntl_wexitstatus($status) !== 0) {
                foreach ($tempFiles as $f) {
                    if (file_exists($f)) {
                        unlink($f);
                    }
                }
                throw new \RuntimeException("Worker exited with error status: $status");
            }
        }

        $result = [];
        foreach ($tempFiles as $tempFile) {
            $partial = $this->decode(file_get_contents($tempFile));
            unlink($tempFile);

            foreach ($partial as $path => $dates) {
                if (isset($result[$path])) {
                    foreach ($dates as $date => $count) {
                        if (isset($result[$path][$date])) {
                            $result[$path][$date] += $count;
                        } else {
                            $result[$path][$date] = $count;
                        }
                    }
                } else {
                    $result[$path] = $dates;
                }
            }

            unset($partial);
        }

        foreach ($result as &$dates) {
            ksort($dates);
        }
        unset($dates);

        $this->writeJson($result, $outputPath);
    }

    private function runWorker(
        string $inputPath,
        int    $start,
        int    $end,
        string $tempFile,
    ): void {
        $data      = [];
        $handle    = fopen($inputPath, 'rb');
        $remaining = $end - $start;

        fseek($handle, $start);

        $leftover = '';

        while ($remaining > 0) {
            $raw = fread($handle, min($this->readChunk, $remaining));

            if ($raw === false || $raw === '') {
                break;
            }

            $remaining -= strlen($raw);
            $pos        = 0;
            $rawLen     = strlen($raw);

            if ($leftover !== '') {
                $nl = strpos($raw, "\n");

                if ($nl === false) {
                    $leftover .= $raw;
                    continue;
                }

                $line     = $leftover . substr($raw, 0, $nl);
                $lineLen  = strlen($line);
                $leftover = '';

                if ($lineLen > 0 && $line[$lineLen - 1] === "\r") {
                    $lineLen--;
                }

                if ($lineLen > self::DOMAIN_LEN + self::TS_LEN) {
                    $date = substr($line, $lineLen - self::TS_LEN, self::DATE_LEN);
                    $path = substr($line, self::DOMAIN_LEN,
                        $lineLen - self::DOMAIN_LEN - self::TS_LEN - 1);

                    $cell = &$data[$path][$date];
                    $cell !== null ? $cell++ : ($cell = 1);
                    unset($cell);
                }

                $pos = $nl + 1;
            }

            while ($pos < $rawLen) {
                $nl = strpos($raw, "\n", $pos);

                if ($nl === false) {
                    $leftover = substr($raw, $pos);
                    break;
                }

                $lineLen = $nl - $pos;

                if ($lineLen > 0 && $raw[$pos + $lineLen - 1] === "\r") {
                    $lineLen--;
                }

                if ($lineLen > self::DOMAIN_LEN + self::TS_LEN) {
                    $date = substr($raw, $pos + $lineLen - self::TS_LEN, self::DATE_LEN);
                    $path = substr($raw, $pos + self::DOMAIN_LEN,
                        $lineLen - self::DOMAIN_LEN - self::TS_LEN - 1);

                    $cell = &$data[$path][$date];
                    $cell !== null ? $cell++ : ($cell = 1);
                    unset($cell);
                }

                $pos = $nl + 1;
            }
        }

        // last line
        if ($leftover !== '') {
            $lineLen = strlen($leftover);

            if ($lineLen > 0 && $leftover[$lineLen - 1] === "\r") {
                $lineLen--;
            }

            if ($lineLen > self::DOMAIN_LEN + self::TS_LEN) {
                $date = substr($leftover, $lineLen - self::TS_LEN, self::DATE_LEN);
                $path = substr($leftover, self::DOMAIN_LEN,
                    $lineLen - self::DOMAIN_LEN - self::TS_LEN - 1);

                $cell = &$data[$path][$date];
                $cell !== null ? $cell++ : ($cell = 1);
                unset($cell);
            }
        }

        fclose($handle);
        file_put_contents($tempFile, $this->encode($data));
    }

    private function writeJson(array $result, string $outputPath): void
    {
        $out = fopen($outputPath, 'wb');
        $buf = "{\n";

        $pathCount = count($result);
        $pathIdx   = 0;

        foreach ($result as $path => $dates) {
            $esc  = str_replace('/', '\\/', $path);
            $buf .= "    \"$esc\": {\n";

            $dateCount = count($dates);
            $dateIdx   = 0;

            foreach ($dates as $date => $count) {
                $buf .= "        \"$date\": $count";
                $buf .= (++$dateIdx < $dateCount) ? ",\n" : "\n";
            }

            $buf .= (++$pathIdx < $pathCount) ? "    },\n" : "    }\n";

            if (strlen($buf) >= self::WRITE_BUFFER) {
                fwrite($out, $buf);
                $buf = '';
            }
        }

        fwrite($out, $buf . '}');
        fclose($out);
    }

    private function getChunks(string $file): array
    {
        $size      = filesize($file);
        $chunkSize = (int) ceil($size / self::WORKER_COUNT);
        $handle    = fopen($file, 'rb');
        $chunks    = [];
        $start     = 0;

        while ($start < $size) {
            $end = min($size, $start + $chunkSize);

            if ($end < $size) {
                fseek($handle, $end);
                fgets($handle);
                $end = ftell($handle);
            }

            $chunks[] = [$start, $end];
            $start    = $end;
        }

        fclose($handle);
        return $chunks;
    }

    private function encode(array $data): string
    {
        return $this->hasIgbinary
            ? igbinary_serialize($data)
            : serialize($data);
    }

    private function decode(string $data): array
    {
        return $this->hasIgbinary
            ? igbinary_unserialize($data)
            : unserialize($data);
    }
}