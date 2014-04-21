use BAZINGA;
use strict;
use warnings;

open (my $fh, "<", "/usr/share/dict/web2a") or die $!;
my @docs = <$fh>;
chomp $_ for @docs;
close $fh;

BAZINGA::index_and_serve(32000,20,10000,\@docs,0);


