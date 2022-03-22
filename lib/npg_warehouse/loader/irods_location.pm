package npg_warehouse::loader::irods_location;

use Moose;
use MooseX::Getopt;
use MooseX::StrictConstructor;
use JSON;
use Log::Log4perl qw(:easy :levels);
use Readonly;
use Data::Dumper;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '0';

Readonly::Scalar my $IRODS_LOCATION_TABLE_NAME => q[SeqProductIrodsLocation];

=head1 NAME

npg_warehouse::loader::irods_location

=head1 SYNOPSIS

=head1 DESCRIPTION

Loads information from a json file into the irods location table in
ml_warehouse database.

=head1 SUBROUTINES/METHODS

=head2 dry_run

Boolean flag preventing changes from being made to ml warehouse

=cut

has 'dry_run'    =>  (isa => 'Bool',
  is                      => 'ro',
  default                 => 0,
  documentation           => 'Boolean option preventing changes from being made to ml warehouse',
                     );

=head2 schema_wh

DBIx schema object for the warehouse database

=cut

has 'schema_wh'  =>  (isa => 'WTSI::DNAP::Warehouse::Schema',
  metaclass               => 'NoGetopt',
  is                      => 'ro',
  lazy_build              => 1,
);
sub _build_schema_wh {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

=head2 path

Path to a json file or directory containing json files with a batch of products
to be added to the table

=cut

has 'path'  =>  (isa => 'Str',
  is                 => 'ro',
  required           => 1,
  documentation      => 'path to a json file or directory containing json files',
);

=head2 verbose

Boolean flag to switch on verbose mode

=cut


has 'verbose' => (isa => 'bool',
  is                  => 'ro',
  documentation       => 'Boolean option to switch on verbose mode',
);

=head2 get_product_locations

Reads json file(s) and returns product information.

File version numbers are included for compatibility with changes to the table
or any other use-case specific json files that are required.

=cut

sub get_product_locations {
  my $self = shift;
  my @json_files = ();
  if (-d $self->path) {
    opendir my $dh, $self->path, or die qq[unable to open directory $self->target];
    my @json_file_names = readdir $dh;
    closedir $dh or die q[unable to close directory];
    @json_files = map { join q[/], $self->path, $_} @json_file_names
  } else {
    push @json_files, $self->path;
  }

  my @locations = ();
  foreach my $file (@json_files) {
    if ($file =~ /.json$/mxs) {
      open my $json_fh, '<:encoding(UTF-8)', $file or die qq[unable to open $file];
      my $data = decode_json <$json_fh>;
      close $json_fh or die q[unable to close file];
      push @locations, @{$data->{products}};
    }
  }
  return \@locations;
}

=head2 load

Creates or updates rows in the seq_product_irods_locations table.

If a row is updated multiple times, the latest update is kept.

=cut

sub load {
  my $self = shift;

  my $locations = $self->get_product_locations();
  my $transaction = sub {
    foreach my $row (@{$locations}) {
      if (!$self->dry_run) {
        my $result = $self->schema_wh->resultset($IRODS_LOCATION_TABLE_NAME)->
        update_or_create($row);
      }
      INFO qq[$IRODS_LOCATION_TABLE_NAME row loaded for id_product $row->{id_product} mapped to irods_collection $row->{irods_root_collection}];
    }
    return;
  };

  $self->schema_wh->txn_do($transaction);
  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::GetOpt

=item MooseX::StrictConstructor

=item JSON

=item Log::Log4Perl

=item Readonly

=item Data::Dumper

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Michael Kubiak

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2021, 2022 Genome Research Ltd.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
