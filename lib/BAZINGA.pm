package BAZINGA;

use 5.012004;
use strict;
use warnings;

our @ISA = qw();

our $VERSION = '0.01';

require XSLoader;
XSLoader::load('BAZINGA', $VERSION);

# Preloaded methods go here.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

BAZINGA - Perl extension for proof of concept pthreads + perl typo detection

=head1 SYNOPSIS

  use BAZINGA;
  # start a bazinga server:
  BAZINGA::index_and_serve($port,    # port for the udp server
	                   $workers, # number of pthreads that will work on the task queue
	                   $max_docs_per_shard,
	                   ["hello world", "hello universe"]);

  # or query a bazinga server:
  print BAZINGA::query("localhost",$port,"univerce",$timeout)

=head1 DESCRIPTION

very simplified proof of concept typo detection using Jaccard score


=head1 INSTALLATION

To install this module type the following:

   perl Makefile.PL
   make
   make test
   make install

=head1 AUTHOR

borislav nikolov, E<lt>jack@sofialondonmoskva.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 by borislav nikolov

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
