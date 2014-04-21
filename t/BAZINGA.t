# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl BAZINGA.t'

#########################
use strict;
use warnings;
use utf8;
use Data::Dumper;
use BAZINGA;

# change 'tests => 1' to 'tests => last_test_to_print';
BEGIN {
    binmode(STDOUT, ":utf8");
    binmode(STDERR, ":utf8");
}
use Test::Exception;
use Test::More;
my $fd = BAZINGA::index_and_serve(32000,10,1000,[ ("hello universe","здрасти","有什么希奇的")x100 ],1);
throws_ok { BAZINGA::index_and_serve(32000,10,1000,[ ("hello universe","здрасти","有什么希奇的")x100 ],1) } qr /bind/;

is(BAZINGA::query("127.0.0.1",32000,"здразти",200),"здрасти");
is(BAZINGA::query("127.0.0.1",32000,"有什么希奇的",200),"有什么希奇的");
is(BAZINGA::query("127.0.0.1",32000,"univerce",200),"universe");
is(BAZINGA::query("127.0.0.1",32000,"hallo univerce",200),"hello universe");

done_testing();

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

