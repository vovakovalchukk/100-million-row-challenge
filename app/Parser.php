<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const int WORKERS       = 10;
    private const int BUFFER_SIZE   = 4 * 1024 * 1024;
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
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($bh, (int)($fileSize * $i / self::WORKERS));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $tmpDir   = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid    = getmypid();
        $children = [];

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
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

                $this->writeCounts($tmpFile, $wCounts);
                exit(0);
            }

            $children[] = [$pid, $tmpFile];
        }

        $counts = $this->parseRange(
            $inputPath,
            $splitPoints[self::WORKERS - 1],
            $splitPoints[self::WORKERS],
            $pathIds, $dateIds, $pathCount, $dateCount, $strposHint,
        );

        foreach ($children as [$cpid, $tmpFile]) {
            pcntl_waitpid($cpid, $status);
            $this->mergeCounts($counts, $tmpFile);
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

            $pos = 0;

            while ($pos < $lastNl) {
                $nl = strpos($chunk, "\n", $pos + $strposHint);

                $counts[
                $pathIds[substr($chunk, $pos + $prefixLen, $nl - $pos - $prefixLen - $suffixLen)]
                + $dateIds[substr($chunk, $nl - $dateOffset, $dateLen)]
                ]++;

                $pos = $nl + 1;
            }
        }

        fclose($handle);
        return $counts;
    }

    private function writeCounts(string $tmpFile, array $counts): void
    {
        $fh        = fopen($tmpFile, 'wb');
        $chunkSize = 65536;

        for ($i = 0, $total = count($counts); $i < $total; $i += $chunkSize) {
            fwrite($fh, pack('V*', ...array_slice($counts, $i, $chunkSize)));
        }

        fclose($fh);
    }

    private function mergeCounts(array &$counts, string $tmpFile): void
    {
        $fh         = fopen($tmpFile, 'rb');
        $chunkBytes = 65536 * 4;
        $j          = 0;

        while (!feof($fh)) {
            $raw = fread($fh, $chunkBytes);
            if ($raw === '' || $raw === false) break;

            foreach (unpack('V*', $raw) as $v) {
                $counts[$j++] += $v;
            }
        }

        fclose($fh);
        unlink($tmpFile);
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

        fwrite($out, '{');

        $firstPath = true;
        $pathCount = count($paths);

        for ($p = 0; $p < $pathCount; $p++) {
            $base      = $p * $dateCount;
            $dateBuf   = '';
            $firstDate = true;

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;

                if (!$firstDate) $dateBuf .= ",\n";
                $firstDate = false;
                $dateBuf  .= '        "20' . $dates[$d] . '": ' . $count;
            }

            if ($firstDate) continue;

            $sep       = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep .
                "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\": {\n" .
                $dateBuf .
                "\n    }"
            );
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}