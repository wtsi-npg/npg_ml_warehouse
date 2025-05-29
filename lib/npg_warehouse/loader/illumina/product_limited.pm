package npg_warehouse::loader::illumina::product_limited;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use Carp;
use List::Util qw/sum/;
use File::Slurp;

use npg_tracking::util::types;
use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::factory::rpt_list;
use npg_qc::autoqc::qc_store;
use npg_warehouse::loader::illumina::autoqc;

my $parent_class = 'npg_warehouse::loader::base';
extends $parent_class;
$parent_class->meta->remove_attribute('schema_npg');
$parent_class->meta->remove_attribute('explain');

with qw/ MooseX::Getopt
         npg_warehouse::loader::illumina::product /;

our $VERSION  = '0';

=head1 NAME

npg_warehouse::loader::illumina::product_limited

=head1 SYNOPSIS

 # Load autoqc results from the given paths:
 npg::warehouse::loader::illumina::product_limited->new(
   autoqc_path => [qw/path1 path2/])->load();

 # Use the QC database as a source of autoqc results:
 npg::warehouse::loader::illumina::product_limited->new(
   composition_path => [qw/path1 path2/])->load();
 npg::warehouse::loader::illumina::product_limited->new(
   rpt_list => [qw/223:1:3;223:2:3 224:1:5/])->load();

=head1 DESCRIPTION

This module loads a summary of autoqc results to ml warehouse.
No attempt to link the results to LIMs data is made. Data is
loaded only to iseq_product_metrics table and its linking table.
Lane-level results are not loaded even if retrieved, unless
the whole lane is a target product. This module is suitable for
loading autoqc summaries for any data, but it's primary purpose
is to provide a way of loading data that exist outside of
the normal run folder structure, i.e. for outcomes of complex
merges where components do not belong to the same run.

Manual QC outcomes will be retrieved and loaded where possible.

In order to load data for merged entities, rows for all
components should exist.

This loader does not need access to npg_tracking database. 

=head1 SUBROUTINES/METHODS

=head2 verbose

=head2 schema_wh

=head2 schema_qc

=cut

has [qw/ +schema_wh +schema_qc /] => (metaclass => 'NoGetopt',);

=head2 rpt_list

An array reference of rpt lists, an optional attribute.
Rpt lists are used to define compositions for which the autoqc
results will be retrieved from the QC database and loaded to the
warehouse.

=cut

has 'rpt_list' => (
  isa       => 'ArrayRef[Str]',
  is        => 'ro',
  required  => 0,
  predicate => 'has_rpt_list',
  documentation => 'An optional list of rpt list strings. ' .
                   'Autoqc results will be retrieved from a database',
);

=head2 composition_path

An array reference of directory paths, an optional attribute.
JSON compositions files found in these directories define
compositions for which the autoqc results will be retrieved from the
QC database and loaded to the warehouse.

=cut

has 'composition_path' => (
  isa       => 'ArrayRef[NpgTrackingDirectory]',
  is        => 'ro',
  required  => 0,
  predicate => 'has_composition_path',
  documentation => 'An optional list of directory paths ' .
                   'where JSON files for compositions can be found' .
                   'Autoqc results will be retrieved from a database',
);

=head2

An array reference of directory paths, an optional attribute.
Autoqc results found in these directories will be loaded to the
warehouse. No access to the QC database will me made.

=cut

has 'autoqc_path' => (
  isa       => 'ArrayRef[NpgTrackingDirectory]',
  is        => 'ro',
  required  => 0,
  predicate => 'has_autoqc_path',
  documentation => 'An optional list of directory paths ' .
                   'where JSON files for autoqc results can be found',
);

=head2 BUILD

Method called before a new object instance is returned to the caller.
Checks that at least one and only one way of defining autoqc results
is supplied to the constructor.

=cut

sub BUILD {
  my $self = shift;

  my @attrs = qw/rpt_list composition_path autoqc_path/;
  my $acount = sum
               map { $self->$_ ? 1 : 0 }
               map { 'has_' . $_  }
               @attrs;
  $acount or croak sprintf 'Either %s should be set.',
                           join q[ or ], @attrs;
  ($acount == 1) or croak sprintf 'Only one of %s can be set',
                                  join q[, ], @attrs;

  return;
}

=head2 load

=cut

sub load {
  my $self = shift;

  my @compositions = ();
  if ($self->has_rpt_list) {
    my $class = 'npg_tracking::glossary::composition::factory::rpt_list';
    @compositions = map { $_->create_composition() }
                    map { $class->new(rpt_list => $_) }
                    @{$self->rpt_list()};
  } elsif ($self->has_composition_path) {
    @compositions = map { npg_tracking::glossary::composition->thaw($_) }
                    map { read_file $_ }
                    glob( join q[ ],
	                  map { "$_/*.collection.json" }
	                  @{$self->composition_path} );
  }

  my $autoqc_store = npg_qc::autoqc::qc_store->new(
    use_db    => !$self->has_autoqc_path,
    verbose   => $self->verbose,
    qc_schema => $self->schema_qc);

  my $collection = $self->has_autoqc_path
    ? $autoqc_store->load_from_path(@{$self->autoqc_path})
    : $autoqc_store->load_from_db_via_composition(\@compositions);

  my $autoqc_data =
    npg_warehouse::loader::illumina::autoqc->new(autoqc_store => $autoqc_store)
                                 ->process($collection);
  my $product_data = $self->product_data($autoqc_data);

  my $count = $self->load_iseqproductmetric_table($product_data);
  if ($self->verbose) {
    warn qq[Loaded $count rows\n];
  }

  return $count;
}

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item List::Util

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Perl6::Slurp

=item npg_tracking::util::types

=item npg_tracking::glossary::composition

=item npg_tracking::glossary::composition::factory::rpt_list

=item npg_qc::autoqc::qc_store

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Limited

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
