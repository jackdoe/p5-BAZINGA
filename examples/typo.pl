#!/usr/bin/perl -CSDA
use BAZINGA;
use utf8;
die "need query" unless @ARGV;
for (1..int($ARGV[1] || 1)) {
	print "$ARGV[0] -> " .  BAZINGA::query("127.0.0.1",32000,$ARGV[0],200) . "\n";
}
