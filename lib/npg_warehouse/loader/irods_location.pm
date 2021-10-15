package npg_warehouse::loader::irods_location;

use Moose;
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

has 'dry_run'    =>  (isa       => 'Bool',
                      is        => 'ro',
                      default   => 0,
                     );

=head2 schema_wh

DBIx schema object for the warehouse database

=cut

has 'schema_wh'  =>  ( isa        => 'WTSI::DNAP::Warehouse::Schema',
                       is         => 'ro',
                       required   => 1,
                     );

=head2 json_file

Name of a json file with a batch of products to be added to the table

=cut

has 'json_file'  =>  ( isa      => 'Str',
                       is       => 'ro',
                       required => 1,
                     );

=head2 products

Reads json file and returns product information.

File version numbers are included for compatibility with changes to the table
or any other use-case specific json files that are required.

=cut

sub products {
  my $self = shift;

  open my $json_fh, '<:encoding(UTF-8)', $self->json_file or die qq[$self->json_file does not exist];
  my $data = decode_json <$json_fh>;
  close $json_fh or die q[unable to close file];
  if ($data->{version} eq '1.0') { # All information in json file
    return $data->{products};
  } else {
    die "data file version number $data->{version} not recognised, this script may be out of date"
  }
}

=head2 load_products

Creates or updates rows in the seq_product_irods_locations table.

If a product is updated multiple times, the latest update is kept.

=cut

sub load_products {
  my $self = shift;

  my $products = $self->products();
  my $transaction = sub {
    foreach my $product (@{$products}) {
      if (!$self->dry_run) {
        my $result = $self->schema_wh->resultset($IRODS_LOCATION_TABLE_NAME)->
        update_or_create($product, { key => 'pi_root_product' });
      }
      INFO qq[$IRODS_LOCATION_TABLE_NAME row loaded for id_product $product->{id_product} mapped to irods_collection $product->{irods_root_collection}];
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

Copyright (C) 2021 Genome Research Ltd.

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
