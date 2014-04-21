use BAZINGA;
die "need query" unless @ARGV;
while(1) {
print "$ARGV[0] -> " .  BAZINGA::query("127.0.0.1",32000,$ARGV[0],200) . "\n";
}
