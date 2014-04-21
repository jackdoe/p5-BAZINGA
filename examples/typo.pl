use BAZINGA;
die "need query" unless @ARGV;
print "$ARGV[0] -> " .  BAZINGA::query("127.0.0.1",32000,$ARGV[0],2000) . "\n";
