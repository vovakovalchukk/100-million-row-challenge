<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function ini_set;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const PHP_INT_MAX;
use const SEEK_CUR;

final class Parser
{
    private const int BUFFER_SIZE   = 163_840;
    private const int DISCOVER_SIZE = 2 * 1024 * 1024;
    private const int PREFIX_LEN    = 25;
    private const int SUFFIX_LEN    = 26;
    private const int WORKERS       = 10;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        ini_set('memory_limit', '-1');

        $fileSize   = filesize($inputPath);
        $numWorkers = self::WORKERS;

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

        $dateIdBytes = [];
        foreach ($dateIds as $date => $id) {
            $dateIdBytes[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $raw = fread($handle, min(self::DISCOVER_SIZE, $fileSize));
        fclose($handle);

        $pathIds      = [];
        $paths        = [];
        $pathCount    = 0;
        $minSlugLen   = PHP_INT_MAX;
        $maxLineLen   = 0;
        $pos          = 0;
        $lastNl       = strrpos($raw, "\n") ?: 0;
        $discoverHint = self::PREFIX_LEN + 1 + self::SUFFIX_LEN;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + $discoverHint);
            if ($nl === false) break;

            $lineLen = $nl - $pos;
            $slugLen = $lineLen - self::PREFIX_LEN - self::SUFFIX_LEN;
            $slug    = substr($raw, $pos + self::PREFIX_LEN, $slugLen);

            if ($slugLen < $minSlugLen) $minSlugLen = $slugLen;
            if ($lineLen > $maxLineLen) $maxLineLen = $lineLen;

            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nl + 1;
        }
        unset($raw);

        foreach (Visit::all() as $visit) {
            $slug    = substr($visit->uri, self::PREFIX_LEN);
            $slugLen = strlen($slug);
            $lineLen = self::PREFIX_LEN + $slugLen + self::SUFFIX_LEN;

            if ($slugLen < $minSlugLen) $minSlugLen = $slugLen;
            if ($lineLen > $maxLineLen) $maxLineLen = $lineLen;

            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        if ($maxLineLen === 0) $maxLineLen = 120;
        if ($minSlugLen === PHP_INT_MAX) $minSlugLen = 5;

        $strposHint     = self::PREFIX_LEN + $minSlugLen + self::SUFFIX_LEN;
        $safeZoneOffset = $maxLineLen * 4;

        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($bh, (int)($fileSize * $i / $numWorkers));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $tmpDir   = sys_get_temp_dir();
        $myPid    = getmypid();
        $childMap = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                gc_disable();
                ini_set('memory_limit', '-1');

                $wCounts = $this->processChunk(
                    $inputPath, $splitPoints[$w], $splitPoints[$w + 1],
                    $pathIds, $dateIdBytes, $pathCount, $dateCount,
                    $strposHint, $safeZoneOffset,
                );

                file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }

            $childMap[$pid] = $tmpFile;
        }

        $counts  = $this->processChunk(
            $inputPath,
            $splitPoints[$numWorkers - 1],
            $splitPoints[$numWorkers],
            $pathIds, $dateIdBytes, $pathCount, $dateCount,
            $strposHint, $safeZoneOffset,
        );

        $n       = $pathCount * $dateCount;
        $pending = count($childMap);

        while ($pending > 0) {
            $pid = pcntl_wait($status);
            if (!isset($childMap[$pid])) continue;

            $tmpFile = $childMap[$pid];
            $wCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $counts[$j++] += $v;
            }
            $pending--;
        }

        $this->writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private function processChunk(
        string $inputPath,
        int    $start,
        int    $end,
        array  $pathIds,
        array  $dateIdBytes,
        int    $pathCount,
        int    $dateCount,
        int    $strposHint,
        int    $safeZoneOffset,
    ): array {
        $buckets   = array_fill(0, $pathCount, '');
        $handle    = fopen($inputPath, 'rb');
        $remaining = $end - $start;

        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $bufSize    = self::BUFFER_SIZE;
        $prefixLen  = self::PREFIX_LEN;
        $suffixLen  = self::SUFFIX_LEN;
        $slugHint   = $strposHint - $prefixLen;

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

            $slugPos      = $prefixLen;
            $safeZoneSlug = $lastNl - $safeZoneOffset + $prefixLen;

            while ($slugPos < $safeZoneSlug) {
                $nl = strpos($chunk, "\n", $slugPos + $slugHint);
                $buckets[$pathIds[substr($chunk, $slugPos, $nl - $slugPos - $suffixLen)]] .= $dateIdBytes[substr($chunk, $nl - 23, 8)];
                $slugPos = $nl + $suffixLen;

                $nl = strpos($chunk, "\n", $slugPos + $slugHint);
                $buckets[$pathIds[substr($chunk, $slugPos, $nl - $slugPos - $suffixLen)]] .= $dateIdBytes[substr($chunk, $nl - 23, 8)];
                $slugPos = $nl + $suffixLen;

                $nl = strpos($chunk, "\n", $slugPos + $slugHint);
                $buckets[$pathIds[substr($chunk, $slugPos, $nl - $slugPos - $suffixLen)]] .= $dateIdBytes[substr($chunk, $nl - 23, 8)];
                $slugPos = $nl + $suffixLen;

                $nl = strpos($chunk, "\n", $slugPos + $slugHint);
                $buckets[$pathIds[substr($chunk, $slugPos, $nl - $slugPos - $suffixLen)]] .= $dateIdBytes[substr($chunk, $nl - 23, 8)];
                $slugPos = $nl + $suffixLen;
            }

            while ($slugPos < $lastNl) {
                $nl = strpos($chunk, "\n", $slugPos + $slugHint);
                if ($nl === false) break;
                $buckets[$pathIds[substr($chunk, $slugPos, $nl - $slugPos - $suffixLen)]] .= $dateIdBytes[substr($chunk, $nl - 23, 8)];
                $slugPos = $nl + $suffixLen;
            }
        }

        fclose($handle);

        $counts = array_fill(0, $pathCount * $dateCount, 0);
        for ($p = 0; $p < $pathCount; $p++) {
            if ($buckets[$p] === '') continue;
            $offset = $p * $dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$p])) as $did => $cnt) {
                $counts[$offset + $did] += $cnt;
            }
        }

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
            $base = $p * $dateCount;
            $buf  = '';
            $sep  = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $buf .= $sep . $datePrefixes[$d] . $count;
                $sep  = ",\n";
            }

            if ($buf === '') continue;

            $sep2      = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep2 .
                "\n    " . $escapedPaths[$p] . ": {\n" .
                $buf .
                "\n    }"
            );
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}