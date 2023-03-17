package npg_warehouse::loader::pacbio::qc_state;

use Moose;
use MooseX::StrictConstructor;
use Readonly;
use LWP::UserAgent;
use HTTP::Request;
use JSON;
use WTSI::DNAP::Warehouse::Schema;

with qw/
        MooseX::Getopt
        npg_warehouse::loader::pacbio::base
       /;

our $VERSION = '0';

Readonly::Scalar my $RUN_WELL_TABLE_NAME => q[PacBioRunWellMetric];
Readonly::Scalar my $LWP_TIMEOUT         => 60;
Readonly::Scalar my $PRODUCT_QC_URI      => q[api/products/qc];
Readonly::Scalar my $QC_TYPE             => q[sequencing];

=head1 NAME

npg_warehouse::loader::pacbio::qc_state

=head1 SYNOPSIS

=head1 DESCRIPTION

  This class retrieves QC state for entities of a PacBio run and undates
  relevant columns of the pac_bio_run_well_metrics and pac_bio_product_metrics
  of ml warehouse database. At the momemt only sequencing QC states for wells
  are retrieved.

  The QC states are retrieve via the API the is provided by the LangQC web
  server, see L<example|https://github.com/wtsi-npg/npg_langqc/blob/devel/docs/qc_process.md#automation>

  This class can be used both as a part of the ml warehouse loader for the
  PacBio data and in isolation. In the latter case a simple wrapper script
  should be used.

  Since this class inherits from the MooseX::Getopt role, the values of its
  scalar attributes, if not set by the caller, will be filled in from the
  content of the @ARGV array.

=head1 SUBROUTINES/METHODS

=cut

has '+pb_api_client' => (
  required => 0,
  metaclass => 'NoGetopt',
);

has '+logger' => (
  metaclass => 'NoGetopt',
);

=head2 mlwh_schema

  DBIx handle for the mlwarehouse schema. Amended here to be built
  lazily if not supplied.

=cut

has '+mlwh_schema' => (
  metaclass  => 'NoGetopt',
  required   => 0,
  lazy_build => 1,
);
sub _build_mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

=head2 server_url

  The URL of the server from which to retrieve information about QC state,
  required.

=cut

has 'server_url' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 1,
  documentation => 'The URL of the LangQC server',
);

=head2 run_name

  The shared Pacbio and SequenceScape/TRACTION run name, required.

=cut

has 'run_name' => (
  isa      => 'Str',
  is       => 'ro',
  required => 1,
  documentation => 'PacBio run name',
);

=head2 product_ids

  A list of product ids for which QC state in ml warehouse has to be updated.
  This list is lazy-built from data available in ml warehouse for the run
  with the name given by the 'run_name' attribute.

=cut

has 'product_ids' => (
  metaclass => 'NoGetopt',
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build_product_ids {
  my $self = shift;

  my @ids = map { $_->id_pac_bio_product() }
            $self->mlwh_schema->resultset($RUN_WELL_TABLE_NAME)->search(
              {pac_bio_run_name => $self->run_name}
            )->all();

  return \@ids;
}

=head2 useragent

  An instance of LWP::UserAgent or its counterpart for testing.

=cut

has 'useragent' => (
  metaclass => 'NoGetopt',
  isa        => 'Object',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build_useragent {
    my $ua = LWP::UserAgent->new();
    $ua->agent(join q[/], __PACKAGE__, $VERSION);
    $ua->timeout($LWP_TIMEOUT);
    return $ua;
}

has '_qc_states' => (
  isa           => 'HashRef',
  is            => 'ro',
  required      => 0,
  lazy_build    => 1,
);
sub _build__qc_states {
  my $self = shift;

  my $decoded = {};

  if (!@{$self->product_ids}) {
    $self->error(sprintf 'List of product IDs for run %s is empty',
      $self->run_name);
    return $decoded;
  }

  my $url = join q[/], $self->server_url, $PRODUCT_QC_URI;
  my $header = [
    'Content-Type' => 'application/json; charset=UTF-8',
    'Accept' => 'application/json'
  ];
  my $encoded_data = encode_json($self->product_ids);
  my $request = HTTP::Request->new('POST', $url, $header, $encoded_data);
  my $response = $self->useragent()->request($request);

  if ($response->is_success()) {
    $decoded = decode_json($response->decoded_content());
  } else {
    $self->error(
      sprintf 'Request to %s failed: %s', $url, $response->status_line()
    );
  }

  return $decoded;
}

=head2 load_qc_state

  Retrieves from LangQC the current QC state of all products associated
  with the run and loads QC state data to relevant PacBio tables.

  Currently only well-level sequencing QC states are retrieved and loaded.

=cut

sub load_qc_state {
  my $self = shift;

  my @ids = keys %{$self->_qc_states()};
  if (not @ids) {
    $self->warn(q[No QC state info is retrieved for run ] . $self->run_name);
    return;
  }

  for my $id (@ids) {

    my $row =
      $self->mlwh_schema->resultset($RUN_WELL_TABLE_NAME)->search(
        {'me.id_pac_bio_product' => $id},
        {join => 'pac_bio_product_metrics'}
      )->next();
    if (!defined $row) {
      $self->error("No mlwh row for product ID $id");
      next;
    }

    # For now we deal with well-level sequencing QC states only.
    my @qc_states = grep { $_->{'qc_type'} eq $QC_TYPE }
                    @{$self->_qc_states()->{$id} || []};

    if (@qc_states == 0) {
        $self->debug("No sequencing QC state for product ID $id");
        next;
    }
    if (@qc_states > 1) {
        $self->error(
          "Multiple sequencing QC states for product ID $id, skipping");
        next;
    }

    my $qc_state = $qc_states[0];
    my $state_is_final = $qc_state->{'is_preliminary'} ? 0 : 1;
    my $outcome = defined $qc_state->{'outcome'} ?
                  ($qc_state->{'outcome'} ? 1 : 0)
                  : undef;
    my $qc_state_data = {
        qc_seq_state          => $qc_state->{'qc_state'},
        qc_seq_state_is_final => $state_is_final,
        qc_seq_date           => $qc_state->{'date_updated'},
    };
    my @undefined = grep { !defined $qc_state_data->{$_} }
                    keys %{$qc_state_data};
    if ( @undefined ) {
      $self->error("Some values are undefined for ID $id, skipping");
      next;
    }
    $qc_state_data->{qc_seq} = $outcome;

    if (not $self->dry_run) {
      $self->debug("Loading QC state for product ID $id");
      ########
      # Update sequencing QC outcome for a well.
      $row->update($qc_state_data);
      ########
      # Update sample final overall QC outcome.
      #
      # We allow overriding final states, so if this qc state is the
      # is the results of a flip from final to preliminary, we have
      # to unset the previous value since we set this value only for
      # the final states.
      $row->pac_bio_product_metrics()->update(
        {'qc' => $state_is_final ? $outcome : undef});
    }
  }

  return;
}

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item Readonly

=item LWP::UserAgent                                                           

=item HTTP::Request
                                                        
=item JSON

=item WTSI::DNAP::Warehouse::Schema

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2023 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
