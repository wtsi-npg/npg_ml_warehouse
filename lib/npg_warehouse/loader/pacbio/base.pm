package npg_warehouse::loader::pacbio::base;

use Moose::Role;
use WTSI::NPG::HTS::PacBio::Sequel::APIClient;
use WTSI::DNAP::Warehouse::Schema;

our $VERSION = '';

has 'dry_run' => 
  (isa           => 'Bool',
   is            => 'ro',
   required      => 1,
   default       => 0,
   documentation => 'dry run mode flag, false by default');

has 'pb_api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Sequel::APIClient',
   is            => 'ro',
   required      => 1,
   documentation => 'A PacBio Sequel API client');

has 'mlwh_schema' =>
  (is            => 'ro',
   isa           => 'WTSI::DNAP::Warehouse::Schema',
   required      => 1,
   documentation => 'A ML warehouse handle');


=head2 fix_date

  Arg [1]    : SMRT Link date string.
  Example    : my($fixed_date) = $self->fix_date($date) 
  Description: Convert SMRT Link date to datetime
  Returntype : String

=cut

sub fix_date {
  my ($self, $date) = @_;

  my $fixed;
  if (defined $date) {
    if($date =~ /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})/) {
      $fixed = $1 .q[ ]. $2;
    } else {
      $self->error(qq[Failed to fix date $date, for loading]); 
    }
  }
  return $fixed;
}

no Moose::Role;

1;

__END__


=head1 NAME

npg_warehouse::loader::pacbio::base

=head1 SYNOPSIS

q=head1 DESCRIPTION

Base class for pacbio data loading.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item WTSI::NPG::HTS::PacBio::Sequel::APIClient

=item WTSI::DNAP::Warehouse::Schema

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2021 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
