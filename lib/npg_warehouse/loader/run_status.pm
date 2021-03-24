package npg_warehouse::loader::run_status;

use Moose;
use MooseX::StrictConstructor;
use DBIx::Class::ResultClass::HashRefInflator;
use Readonly;

use npg_tracking::Schema;

our $VERSION = '0';

Readonly::Array  our @RUN_STATUS_TABLES   => qw/ RunStatusDict RunStatus /;

=head1 NAME

npg_warehouse::loader::run_status

=head1 SYNOPSIS

=head1 DESCRIPTION

Copies (updates and inserts) all runs status history information from the
npg tracking to ml warehouse database.

=head1 SUBROUTINES/METHODS

=head2 schema_npg

DBIx schema object for the NPG tracking database

=cut

has 'schema_npg' =>  ( isa        => 'npg_tracking::Schema',
                       is         => 'ro',
                       required   => 0,
                       lazy_build => 1,
                     );
sub _build_schema_npg {
  return npg_tracking::Schema->connect();
}

=head2 schema_wh

DBIx schema object for the warehouse database

=cut

has 'schema_wh'   =>  ( isa        => 'WTSI::DNAP::Warehouse::Schema',
                        is         => 'ro',
                        required   => 1,
                      );

has '_prefix'     =>  ( isa        => 'Str',
                        is         => 'ro',
                        required   => 0,
                        default    => 'Iseq',
                      );

######
# Copies a table from the npg tracking to the warehouse database.
# Assumes that warehouse table name is the same as in tracking
# with an addition of a prefix.
sub _copy_table {
  my ($self, $table) = @_;

  my $rs_npg = $self->schema_npg->resultset($table);
  $rs_npg->result_class('DBIx::Class::ResultClass::HashRefInflator');
  my $rs_wh = $self->schema_wh->resultset($self->_prefix . $table);
  while (my $row_hash = $rs_npg->next) {
    delete $row_hash->{id_user};
    $rs_wh->update_or_create($row_hash);
  }
  return;
}

=head2 copy_npg_tables

Copies all run statuses and a dictionary from the npg tracking to the warehouse database.

=cut

sub copy_npg_tables {
  my $self = shift;
  my $transaction = sub {
    foreach my $table (@RUN_STATUS_TABLES) {
      $self->_copy_table($table);
    }
  };
  $self->schema_wh->txn_do($transaction);
  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Readonly

=item Moose

=item MooseX::StrictConstructor

=item DBIx::Class::ResultClass::HashRefInflator;

=item npg_tracking::Schema

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014,2021 Genome Research Ltd.

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
