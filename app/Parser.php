<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int BUFFER_SIZE   = 8 * 1024 * 1024;
    private const int DISCOVER_SIZE = 2 * 1024 * 1024;

    private const int PREFIX_LEN  = 25;
    private const int SUFFIX_LEN  = 26;
    private const int DATE_OFFSET = 23;
    private const int DATE_LEN    = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize = filesize($inputPath);

        if (PHP_OS_FAMILY === 'Darwin') {
            $numWorkers = 8;
        } else {
            $numWorkers = max(2, (int)trim(shell_exec('nproc 2>/dev/null') ?: '2'));
        }

        $dateIds   = [];
        $dates     = [];
        $dateCount = 0;

        for ($y = 20; $y <= 27; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr  = ($m < 10 ? '0' : '') . $m;
                $ymStr = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key               = $ymStr . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key]     = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, min(self::DISCOVER_SIZE, $fileSize));
        fclose($handle);

        $pathIds    = [];
        $paths      = [];
        $pathCount  = 0;
        $pos        = 0;
        $lastNl     = strrpos($raw, "\n") ?: 0;
        $minLineLen = PHP_INT_MAX;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + self::PREFIX_LEN + 1 + self::SUFFIX_LEN);
            if ($nl === false) break;

            $lineLen = $nl - $pos;
            if ($lineLen < $minLineLen) {
                $minLineLen = $lineLen;
            }

            $slug = substr($raw, $pos + self::PREFIX_LEN,
                $nl - $pos - self::PREFIX_LEN - self::SUFFIX_LEN);

            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount * $dateCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nl + 1;
        }
        unset($raw);

        $strposHint = $minLineLen !== PHP_INT_MAX
            ? $minLineLen
            : self::PREFIX_LEN + 1 + self::SUFFIX_LEN;

        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($bh, (int)($fileSize * $i / $numWorkers));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $tmpDir   = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid    = getmypid();
        $children = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                gc_disable();
                ini_set('memory_limit', '-1');

                $wCounts = $this->parseRange(
                    $inputPath, $splitPoints[$w], $splitPoints[$w + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount, $strposHint,
                );

                file_put_contents($tmpFile, pack('V*', ...$wCounts));
                exit(0);
            }

            $children[] = [$pid, $tmpFile];
        }

        $counts = $this->parseRange(
            $inputPath,
            $splitPoints[$numWorkers - 1],
            $splitPoints[$numWorkers],
            $pathIds, $dateIds, $pathCount, $dateCount, $strposHint,
        );

        $pending = $children;
        $n       = $pathCount * $dateCount;

        while (!empty($pending)) {
            $anyDone = false;

            foreach ($pending as $key => [$cpid, $tmpFile]) {
                $ret = pcntl_waitpid($cpid, $status, WNOHANG);
                if ($ret > 0) {
                    $wCounts = unpack('V*', file_get_contents($tmpFile));
                    unlink($tmpFile);
                    for ($j = 0, $k = 1; $j < $n; $j++, $k++) {
                        $counts[$j] += $wCounts[$k];
                    }
                    unset($pending[$key]);
                    $anyDone = true;
                    break;
                }
            }

            if (!$anyDone && !empty($pending)) {
                reset($pending);
                $key              = key($pending);
                [$cpid, $tmpFile] = $pending[$key];
                pcntl_waitpid($cpid, $status);
                $wCounts = unpack('V*', file_get_contents($tmpFile));
                unlink($tmpFile);
                for ($j = 0, $k = 1; $j < $n; $j++, $k++) {
                    $counts[$j] += $wCounts[$k];
                }
                unset($pending[$key]);
            }
        }

        $this->writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private function parseRange(
        string $inputPath,
        int    $start,
        int    $end,
        array  $pathIds,
        array  $dateIds,
        int    $pathCount,
        int    $dateCount,
        int    $strposHint,
    ): array {
        $counts    = array_fill(0, $pathCount * $dateCount, 0);
        $handle    = fopen($inputPath, 'rb');
        $remaining = $end - $start;

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $bufSize    = self::BUFFER_SIZE;
        $prefixLen  = self::PREFIX_LEN;
        $suffixLen  = self::SUFFIX_LEN;
        $dateOffset = self::DATE_OFFSET;
        $dateLen    = self::DATE_LEN;
        $leftover   = '';

        while ($remaining > 0) {
            $toRead = $remaining > $bufSize ? $bufSize : $remaining;
            $chunk  = fread($handle, $toRead);
            if ($chunk === false || $chunk === '') break;

            $chunkLen   = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                fseek($handle, -$chunkLen, SEEK_CUR);
                $remaining += $chunkLen;
                break;
            }

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            if ($leftover !== '') {
                $nl      = strpos($chunk, "\n");
                $line    = $leftover . substr($chunk, 0, $nl);
                $lineLen = strlen($line);
                $leftover = '';

                $counts[
                $pathIds[substr($line, $prefixLen, $lineLen - $prefixLen - $suffixLen)]
                + $dateIds[substr($line, $lineLen - $dateOffset, $dateLen)]
                ]++;
            }

            $pos = 0;

            while ($pos < $lastNl) {
                $nl = strpos($chunk, "\n", $pos + $strposHint);

                $counts[
                $pathIds[substr($chunk, $pos + $prefixLen, $nl - $pos - $prefixLen - $suffixLen)]
                + $dateIds[substr($chunk, $nl - $dateOffset, $dateLen)]
                ]++;

                $pos = $nl + 1;
            }

            $leftover = $tail > 0 ? '' : substr($chunk, $lastNl + 1);
        }

        fclose($handle);
        return $counts;
    }

    private function writeJson(
        string $outputPath,
        array  $counts,
        array  $paths,
        array  $dates,
        int    $dateCount,
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1024 * 1024);

        $pathCount    = count($paths);
        $datePrefixes = [];
        $escapedPaths = [];

        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "20' . $dates[$d] . '": ';
        }

        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        fwrite($out, '{');
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base        = $p * $dateCount;
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count !== 0) {
                    $dateEntries[] = $datePrefixes[$d] . $count;
                }
            }

            if (empty($dateEntries)) continue;

            $sep       = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep .
                "\n    " . $escapedPaths[$p] . ": {\n" .
                implode(",\n", $dateEntries) .
                "\n    }"
            );
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}