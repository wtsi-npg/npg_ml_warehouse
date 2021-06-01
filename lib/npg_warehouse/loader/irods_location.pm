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

=head2 update

Boolean flag allowing update of currently present rows

=cut

has 'update'     =>  (isa       => 'Bool',
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

Reads json file and returns product information

=cut

sub products {
  my $self = shift;

  open my $json_fh, '<:encoding(UTF-8)', $self->json_file or die qq[$self->json_file does not exist];
  my $data = decode_json <$json_fh>;
  close $json_fh or die q[unable to close file];
  if ($data->{version} eq '0.1') { # All information in json file
    return $data->{products};
  } else {
    die "data file version number $data->{version} not recognised, this script may be out of date"
  }
}

=head2 load_products

Loads products into the table

=cut

sub load_products {
  my $self = shift;

  my $products = $self->products();
  foreach my $product (@{$products}){
    INFO qq[loading id_product = $product->{id_product} mapped to irods_collection $product->{irods_root_collection}];
    my $row = $self->schema_wh->resultset($IRODS_LOCATION_TABLE_NAME)->
      find_or_new($product, {key => 'pi_root_product'});
    if ($row->in_storage && $self->update) {
      $row->set_columns($product);
      INFO q[row already present, updating values:] . Dumper {$row->get_dirty_columns};
      $row->update;
    } elsif ($row->in_storage) {
      INFO q[row already present, not updating];
    } elsif (!$self->dry_run){
      $row->insert;
    }
  }
  return;
}

=head2 delete_products

Deletes products from the table

=cut

sub delete_products {
  my $self = shift;

  my $products = $self->products();
  foreach my $product (@{$products}){
    INFO qq[deleting id_product = $product->{id_product} mapped to irods_collection $product->{irods_root_collection}];
    my $row = $self->schema_wh->resultset($IRODS_LOCATION_TABLE_NAME)->
      find($product, {key => 'pi_root_product'});
    if (!$row->in_storage) {
      INFO qq[id_product $product->{id_product}, mapped to irods_path $product->{irods_root_collection} not in table];
    } elsif (!$self->dry_run){
      $row->delete;
    }

  }
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
