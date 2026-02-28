<?php

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function chr;
use function count;
use function fclose;
use function fflush;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function flock;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function implode;
use function intdiv;
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

use const LOCK_EX;
use const LOCK_UN;
use const SEEK_CUR;

final class Parser
{
    private const int BUFFER_SIZE = 163_840;
    private const int PREFIX_LEN  = 25;
    private const int WORKERS     = 8;
    private const int CHUNKS      = 16;

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize   = filesize($inputPath);
        $numWorkers = self::WORKERS;
        $numChunks  = self::CHUNKS;

        $dateIds   = [];
        $dates     = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            $yStr = (string)$y;
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => $y === 24 ? 29 : 28,
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

        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::PREFIX_LEN);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numChunks; $i++) {
            fseek($bh, intdiv($fileSize * $i, $numChunks));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $tmpDir    = sys_get_temp_dir();
        $myPid     = getmypid();
        $tmpPrefix = $tmpDir . '/p100m_' . $myPid;
        $queueFile = $tmpPrefix . '_queue';

        file_put_contents($queueFile, pack('V', 0));

        $childMap = [];

        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tmpFile = $tmpPrefix . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                $buckets = array_fill(0, $pathCount, '');
                $fh      = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                $qf      = fopen($queueFile, 'c+b');

                while (true) {
                    $ci = self::grabChunk($qf, $numChunks);
                    if ($ci === -1) break;
                    self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateIdBytes, $buckets);
                }

                fclose($qf);
                fclose($fh);

                $counts = self::bucketsToCounts($buckets, $pathCount, $dateCount);
                file_put_contents($tmpFile, pack('v*', ...$counts));
                exit(0);
            }

            $childMap[$pid] = $tmpFile;
        }

        $buckets = array_fill(0, $pathCount, '');
        $fh      = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $qf      = fopen($queueFile, 'c+b');

        while (true) {
            $ci = self::grabChunk($qf, $numChunks);
            if ($ci === -1) break;
            self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateIdBytes, $buckets);
        }

        fclose($qf);
        fclose($fh);

        $counts = self::bucketsToCounts($buckets, $pathCount, $dateCount);

        while ($childMap) {
            $pid = pcntl_wait($status);
            if (!isset($childMap[$pid])) continue;

            $tmpFile     = $childMap[$pid];
            $childCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            unset($childMap[$pid]);

            $j = 0;
            foreach ($childCounts as $v) {
                $counts[$j++] += $v;
            }
        }

        unlink($queueFile);

        self::writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private static function grabChunk($qf, $numChunks)
    {
        flock($qf, LOCK_EX);
        fseek($qf, 0);
        $idx = unpack('V', fread($qf, 4))[1];
        if ($idx >= $numChunks) {
            flock($qf, LOCK_UN);
            return -1;
        }
        fseek($qf, 0);
        fwrite($qf, pack('V', $idx + 1));
        fflush($qf);
        flock($qf, LOCK_UN);
        return $idx;
    }

    private static function fillBuckets($handle, $start, $end, $pathIds, $dateIdBytes, &$buckets)
    {
        fseek($handle, $start);

        $processed = 0;
        $toProcess = $end - $start;
        $bufSize   = self::BUFFER_SIZE;
        $prefixLen = self::PREFIX_LEN;

        while ($processed < $toProcess) {
            $remaining = $toProcess - $processed;
            $chunk     = fread($handle, $remaining > $bufSize ? $bufSize : $remaining);
            if (!$chunk) break;

            $chunkLen = strlen($chunk);
            $lastNl   = strrpos($chunk, "\n");
            if ($lastNl === false) continue;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }
            $processed += $lastNl + 1;

            $p     = $prefixLen;
            $fence = $lastNl - 720;

            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateIdBytes[substr($chunk, $sep + 3, 8)];
                $p = $sep + 52;
            }
        }
    }

    private static function bucketsToCounts(&$buckets, $pathCount, $dateCount)
    {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $base   = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $did => $cnt) {
                    $counts[$base + $did] += $cnt;
                }
            }
            $base += $dateCount;
        }
        return $counts;
    }

    private static function writeJson($outputPath, $counts, $paths, $dates, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

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
        $base      = 0;

        for ($p = 0; $p < $pathCount; $p++) {
            $dateEntries = [];

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count !== 0) {
                    $dateEntries[] = $datePrefixes[$d] . $count;
                }
            }

            if (empty($dateEntries)) {
                $base += $dateCount;
                continue;
            }

            $sep2      = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep2 .
                "\n    " . $escapedPaths[$p] . ": {\n" .
                implode(",\n", $dateEntries) .
                "\n    }"
            );

            $base += $dateCount;
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}