# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl BAZINGA.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;

use Test::More tests => 2;

BEGIN { use_ok('BAZINGA') };
use Data::Dumper;
$SIG{CHLD} = 'IGNORE';
$SIG{PIPE} = 'IGNORE';

my $pid = fork();
if ($pid) {
   BAZINGA::index_and_serve(32000,10,1000,[("asdasd") x 100000]);
}
sleep(1);
is(BAZINGA::query("127.0.0.1",32000,"asxasd",100),"asdasd");
sleep(1);
kill(9,$pid);

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

