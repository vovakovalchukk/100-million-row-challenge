<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int WORKERS      = 6;
    private const int BUFFER_SIZE  = 8 * 1024 * 1024;
    private const int WRITE_BUFFER = 8 * 1024 * 1024;

    private const int DOMAIN_LEN = 19;
    private const int TS_LEN     = 25;
    private const int DATE_LEN   = 10;
    private const int MIN_LINE   = 45;

    private bool $hasIgbinary;

    public function __construct()
    {
        $this->hasIgbinary = function_exists('igbinary_serialize');
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '-1');

        $fileSize    = filesize($inputPath);
        $workerCount = self::WORKERS;

        $splitPoints = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workerCount; $i++) {
            $offset = (int)($fileSize * $i / $workerCount);
            fseek($handle, $offset);
            fgets($handle);
            $splitPoints[] = ftell($handle);
        }
        fclose($handle);
        $splitPoints[] = $fileSize;

        $children = [];
        for ($i = 1; $i < $workerCount; $i++) {
            $pipes = stream_socket_pair(
                STREAM_PF_UNIX,
                STREAM_SOCK_STREAM,
                STREAM_IPPROTO_IP,
            );

            $pid = pcntl_fork();

            if ($pid === 0) {
                ini_set('memory_limit', '-1');
                fclose($pipes[0]);

                $data   = $this->processChunk($inputPath, $splitPoints[$i], $splitPoints[$i + 1]);
                $packed = $this->encode($data);
                unset($data);

                fwrite($pipes[1], $packed);
                fclose($pipes[1]);
                exit(0);
            }

            fclose($pipes[1]);
            $children[] = ['pid' => $pid, 'pipe' => $pipes[0]];
        }

        $result = $this->processChunk($inputPath, $splitPoints[0], $splitPoints[1]);

        foreach ($children as $child) {
            $packed = stream_get_contents($child['pipe']);
            fclose($child['pipe']);
            pcntl_waitpid($child['pid'], $status);

            $partial = $this->decode($packed);
            unset($packed);

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

    private function processChunk(string $inputPath, int $start, int $end): array
    {
        $data      = [];
        $handle    = fopen($inputPath, 'rb');
        $remaining = $end - $start;

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $bufSize   = self::BUFFER_SIZE;
        $domainLen = self::DOMAIN_LEN;
        $tsLen     = self::TS_LEN;
        $dateLen   = self::DATE_LEN;
        $minLine   = self::MIN_LINE;

        $leftover = '';

        while ($remaining > 0) {
            $raw = fread($handle, min($bufSize, $remaining));
            if ($raw === false || $raw === '') break;

            $remaining -= strlen($raw);
            $rawLen     = strlen($raw);
            $pos        = 0;

            if ($leftover !== '') {
                $nl = strpos($raw, "\n");
                if ($nl === false) { $leftover .= $raw; continue; }

                $line    = $leftover . substr($raw, 0, $nl);
                $lineLen = strlen($line);
                $leftover = '';

                if ($lineLen > $minLine) {
                    $date = substr($line, $lineLen - $tsLen, $dateLen);
                    $path = substr($line, $domainLen, $lineLen - $domainLen - $tsLen - 1);
                    $cell = &$data[$path][$date];
                    $cell !== null ? $cell++ : ($cell = 1);
                    unset($cell);
                }
                $pos = $nl + 1;
            }

            while ($pos < $rawLen) {
                $nl = strpos($raw, "\n", $pos);
                if ($nl === false) { $leftover = substr($raw, $pos); break; }

                if ($nl - $pos > $minLine) {
                    $date = substr($raw, $nl - $tsLen, $dateLen);
                    $path = substr($raw, $pos + $domainLen, $nl - $pos - $domainLen - $tsLen - 1);

                    $cell = &$data[$path][$date];
                    $cell !== null ? $cell++ : ($cell = 1);
                    unset($cell);
                }

                $pos = $nl + 1;
            }
        }

        if ($leftover !== '') {
            $lineLen = strlen($leftover);
            if ($lineLen > $minLine) {
                $date = substr($leftover, $lineLen - $tsLen, $dateLen);
                $path = substr($leftover, $domainLen, $lineLen - $domainLen - $tsLen - 1);
                $cell = &$data[$path][$date];
                $cell !== null ? $cell++ : ($cell = 1);
                unset($cell);
            }
        }

        fclose($handle);
        return $data;
    }

    private function writeJson(array $result, string $outputPath): void
    {
        $out    = fopen($outputPath, 'wb');
        $buf    = "{\n";
        $bufLen = 2;

        $pathCount = count($result);
        $pathIdx   = 0;

        foreach ($result as $path => $dates) {
            $esc     = str_replace('/', '\\/', $path);
            $header  = "    \"$esc\": {\n";
            $buf    .= $header;
            $bufLen += strlen($header);

            $dateCount = count($dates);
            $dateIdx   = 0;

            foreach ($dates as $date => $count) {
                $line    = "        \"$date\": $count";
                $line   .= (++$dateIdx < $dateCount) ? ",\n" : "\n";
                $buf    .= $line;
                $bufLen += strlen($line);
            }

            $closing  = (++$pathIdx < $pathCount) ? "    },\n" : "    }\n";
            $buf     .= $closing;
            $bufLen  += strlen($closing);

            if ($bufLen >= self::WRITE_BUFFER) {
                fwrite($out, $buf);
                $buf    = '';
                $bufLen = 0;
            }
        }

        fwrite($out, $buf . '}');
        fclose($out);
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