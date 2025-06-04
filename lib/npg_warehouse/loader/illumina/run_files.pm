package npg_warehouse::loader::illumina::run_files;

use namespace::autoclean;
use Moose;
use Cwd qw/abs_path/;
use Perl6::Slurp;

use WTSI::DNAP::Warehouse::Schema;

with qw/ MooseX::Getopt
         WTSI::DNAP::Utilities::Loggable
         npg_tracking::glossary::run /;

our $VERSION  = '0';

=head1 NAME

npg_warehouse::loader::illumina::run_files

=head1 SYNOPSIS

  my $file = npg_warehouse::loader::illumina::run_files->new(
    id_run => 4567,
    path_glob = '/home/my/{r,R}unParameters.xml'
  )->load;
  print "Loaded file $file\n";

  # The above is equivalent to:

  my $path = '/home/my/runParameters.xml';
  $path = -e $path ? $path : '/home/my/RunParameters.xml';
  $file = npg_warehouse::loader::illumina::run_files->new(
    id_run => 4567,
    path_glob = $path
  )->load;

=head1 DESCRIPTION

Loader for the Illumina run parameters files, potentially extendable to other
types of files with run-level information. The contents of the file is loaded
to the C<iseq_run_info> ml warehouse table. Some values from the file are
extracted to the columns of the C<iseq_run> table. 

=head1 SUBROUTINES/METHODS

=head2 id_run

Inherited from C<npg_tracking::glossary::run>.
NPG run identifier, an attribute, required.

=cut

has '+id_run' => (
  documentation => 'NPG integer run identifier, required',
);

has '+logger' => (
  metaclass   => 'NoGetopt',
);

=head2 path_glob

A string representing a path glob, which should resolve to a single file.

=cut

has 'path_glob' => (
  isa         => 'Str',
  is          => 'ro',
  required    => 1,
  documentation => 'A glob expression for a file to load or a path',
);

=head2 schema_wh

DBIx schema object for the warehouse database. Will be built if not supplied.

=cut

has 'schema_wh'  =>  (
  isa         => 'WTSI::DNAP::Warehouse::Schema',
  metaclass   => 'NoGetopt',
  is          => 'ro',
  required    => 0,
  lazy_build  => 1,
);
sub _build_schema_wh {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

has '_file_path' => (
  isa         => 'Str',
  is          => 'ro',
  required    => 0,
  lazy_build  => 1,
);
sub _build__file_path {
  my $self = shift;

  if ($self->path_glob eq q[]) {
    $self->logcroak('Non-empty path glob is required');
  }

  my $glob = $self->path_glob;
  ######
  # Yes, 'abs_path $_', whatever perlcritic says.
  #
  # Have to check for file existence since using a glob expression
  # like '/home/my/{r,R}unParameters.xml' results in both versions of
  # the path returned. Using a glob expression like
  # '/home/my/{r,R}unParam*.xml' returns a single existing path.
  #
  my @files = map { abs_path $_ } grep { -e } grep { not -d } glob $glob;
  if (not @files) {
    $self->logcroak('No files found');
  }
  if (@files > 1) {
    $self->logcroak('Multiple files found');
  }

  return $files[0];
}

=head2 load

Method that loads a file with run parameters to the database. 
Error if the file does not contain a C<RunParameters> XML element.
Returns the path of the file which contents has been loaded.

=cut

sub load {
  my $self = shift;

  my $path = $self->_file_path;
  my $contents = slurp $path;

  if ((! defined $contents) || ($contents eq q[])) {
    $self->logcroak("File $path is empty");
  }
  # Is this a run papameters file?
  if ($contents !~ /\<RunParameters.*\>/smx) {
    $self->logcroak("File $path is not an Illumina run params file");
  }

  $self->schema_wh->resultset('IseqRunInfo')->update_or_create(
    {id_run => $self->id_run, run_parameters_xml => $contents}
  );

  return $path;
}

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Cwd

=item Perl6::Slurp

=item MooseX::Getopt

=item WTSI::DNAP::Warehouse::Schema

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::glossary::run

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2022 Genome Research Ltd.

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
