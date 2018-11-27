use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use Moose::Meta::Class;

use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use npg_testing::db;
use t::util;

use_ok('npg_warehouse::loader::fqc');

my $compos_pkg = 'npg_tracking::glossary::composition';
my $compon_pkg = 'npg_tracking::glossary::composition::component::illumina';

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_qc;

my @qc_types = qw/qc qc_lib qc_seq/;

lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]) } 'qc test db created';

my $rs = $schema_qc->resultset('MqcOutcomeEnt');

for my $p ((1 .. 6)) {
  my $q = {id_run => 3, position => $p};
  $q->{'id_seq_composition'} =
    t::util::find_or_save_composition($schema_qc, $q);
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  $rs->create($q);
}

my $srs = $rs->search({id_run => 3});
while (my $row = $srs->next) {
  my $p = $row->position;
  if ($p ==2 || $p == 5) {
    my $id = ($p == 2)
      ? 3  # 'Accepted final'
      : 4; # 'Rejected final';
    $row->update({id_mqc_outcome => $id, reported => $row->get_time_now()});
  }
}

subtest 'object initialization and input checking' => sub {
  plan tests => 4;

  my $mqc;
  lives_ok {$mqc = npg_warehouse::loader::fqc->new(schema_qc => $schema_qc)}
    'object instantiated';
  isa_ok ($mqc, 'npg_warehouse::loader::fqc');
  throws_ok { $mqc->retrieve_lane_outcome() }
    qr/Composition object is missing/, 'no composition object - error';
  throws_ok { $mqc->retrieve_outcomes() }
    qr/Composition object is missing/, 'no composition object - error';
};

subtest 'retrieve lane seq outcome' => sub {
  plan tests => 4; 

  my $mqc  = npg_warehouse::loader::fqc->new( 
                                schema_qc => $schema_qc);

  my $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 1)]);
  is_deeply ($mqc->retrieve_lane_outcome($c), {qc_seq => undef},
    'undefined for a non-final outcome');
  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 2)]);
  is_deeply ($mqc->retrieve_lane_outcome($c), {qc_seq => 1}, '1 for accepted final');
  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 5)]);
  is_deeply ($mqc->retrieve_lane_outcome($c), {qc_seq => 0}, '0 for rejected final');
  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 8)]);
  is_deeply ($mqc->retrieve_lane_outcome($c), {qc_seq => undef}, 'undef for no record');  
};

subtest 'retrieve outcomes for a lane' => sub {
  plan tests => 4;

  my $outcomes = {};
  $outcomes->{qc} = undef;
  $outcomes->{qc_seq} = undef;
  $outcomes->{qc_lib} = undef;

  my $mqc  = npg_warehouse::loader::fqc->new( 
                     schema_qc => $schema_qc);

  my $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 1)]);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'non-final seq outcome, all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 8)]);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'no record, all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 2)]);
  $outcomes->{qc_seq} = 1;
  $outcomes->{qc} = 1;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'accepted final seq outcome result');

  $outcomes->{qc_seq} = 0;
  $outcomes->{qc} = 0;
  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 5)]);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'rejected final seq outcome result'); 
};

subtest 'retrieve outcomes for a one component plex' => sub {
  plan tests => 18;

  my $rsl = $schema_qc->resultset('MqcLibraryOutcomeEnt');

  my $outcomes = {};
  $outcomes->{qc} = undef;
  $outcomes->{qc_seq} = undef;
  $outcomes->{qc_lib} = undef;
  my $q = {};

  my $mqc  = npg_warehouse::loader::fqc->new( 
                     schema_qc => $schema_qc);

  my $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 8, tag_index => 1)]);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes, 'no record - all undefined');

  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 8, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  $rsl->create($q);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'no record for seq, no final record for lib - all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 7, tag_index => 1)]);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes, 'no record - all undefined');
  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 7, tag_index => 1});
  $q->{'id_mqc_outcome'} = 3; #'Accepted final' 
  $rsl->create($q);
  throws_ok {$mqc->retrieve_outcomes($c)}
    qr/Inconsistent qc outcomes/,
    'error for final lib outcome with no seq outcome';

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 1, tag_index => 1)]);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'no lib outcome, non-final seq outcome - all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 3, tag_index => 1)]);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'no lib outcome, non-final seq outcome - all undefined');
  $q = {};
  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 3, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  my $o = $rsl->create($q);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'both lib and seq outcomes non-final - all undefined');

  $o->delete();
  $q->{'id_mqc_outcome'} = 3; #'Accepted final'
  $rsl->create($q);
  throws_ok {$mqc->retrieve_outcomes($c)}
    qr/Inconsistent qc outcomes/,
    'error for final lib outcome wiht prelim seq outcome';

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 5, tag_index => 1)]);
  $outcomes->{qc_seq} = 0;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'final fail seq outcome, no lib outcome - overall undefined');

  $q = {};
  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 5, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  $o = $rsl->create($q);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq outcome final, lib not final - overall undefined');

  $o->delete();
  $q->{'id_mqc_outcome'} = 3; #'Accepted final';
  $o = $rsl->create($q);
  $outcomes->{qc_lib} = 1;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq and lib outcomes final, but opposite - overall fail');

  $o->delete();
  $q->{'id_mqc_outcome'} = 4; #'Rejected final';
  $o = $rsl->create($q);
  $outcomes->{qc_lib} = 0;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq and lib outcomes final all fail - all fail');

  $o->delete();
  $q->{'id_mqc_outcome'} = 6; #'Undecided final';
  $o = $rsl->create($q);
  $outcomes->{qc_lib} = undef;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq and lib outcomes final, lib undef - overall fail');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 2, tag_index => 1)]);
  $outcomes->{qc_seq} = 1;
  $outcomes->{qc} = undef;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'final seq outcome, no lib outcome - overall undefined');

  $q = {};
  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 2, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  $o = $rsl->create($q);
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq outcome pass final, lib not final - overall undefined');

  $o->delete();
  $q->{'id_mqc_outcome'} = 3; #'Accepted final';
  $o = $rsl->create($q);
  $outcomes->{qc_lib} = 1;
  $outcomes->{qc} = 1;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq and lib outcomes final - all pass');

  $o->delete();
  $q->{'id_mqc_outcome'} = 4; #'Rejected final';
  $o = $rsl->create($q);
  $outcomes->{qc_lib} = 0;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq and lib outcomes final, lib fail - lib and qc fail');

  $o->delete();
  $q->{'id_mqc_outcome'} = 6; #'Undecided final';
  $o = $rsl->create($q);
  $outcomes->{qc_lib} = undef;
  $outcomes->{qc} =undef;
  is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
    'seq and lib outcomes final, lib undef - lib and qc undef');
};

subtest 'retrieve outcomes for a multi component plex' => sub {
  plan tests => 24;

  my $id_run = 4;
  my @lane_rows = ();
  for my $p ((1 .. 4)) {
    my $q = {id_run => $id_run, position => $p};
    $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, $q);
    $q->{'id_mqc_outcome'} = 3; #'Accepted final' 
    push @lane_rows, $rs->create($q);
  }

  my $rsl = $schema_qc->resultset('MqcLibraryOutcomeEnt');
  my @compositions = ();
  for my $i ((1 .. 6)) {
    my @queries =
      map { {id_run => $id_run, position => $_, tag_index => $i} }
      (1 .. 4);
    my $q = {};
    $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, @queries);
    $q->{'id_mqc_outcome'} = $i; 
    my $row = $rsl->create($q);
    push @compositions, $row->composition;   
  }

  my $mqc  = npg_warehouse::loader::fqc->new( 
                     schema_qc => $schema_qc);

  for my $c (@compositions) {
    my $outcomes = {};
    $outcomes->{qc}     = undef;
    $outcomes->{qc_seq} = 1;
    $outcomes->{qc_lib} = undef;
    my $i = $c->get_component(0)->tag_index;
    if ($i == 3) {
      $outcomes->{qc}     = 1;
      $outcomes->{qc_lib} = 1;
    } elsif ($i == 4) {
      $outcomes->{qc}     = 0;
      $outcomes->{qc_lib} = 0;
    }
    is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
      "correct outcome for plex $i when all lanes pass");    
  }

  for my $lane (@lane_rows) {
    $lane->update({'id_mqc_outcome' => 4});
  }
  for my $c (@compositions) {
    my $outcomes = {};
    $outcomes->{qc}     = 0;
    $outcomes->{qc_seq} = 0;
    $outcomes->{qc_lib} = undef;
    my $i = $c->get_component(0)->tag_index;
    if ($i == 3) {
      $outcomes->{qc_lib} = 1;
    } elsif ($i == 4) {
      $outcomes->{qc_lib} = 0;
    }
    is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
      "correct outcome for plex $i when all lanes fail");    
  }

  $lane_rows[0]->update({'id_mqc_outcome' => 3});
  for my $c (@compositions) {
    my $outcomes = {};
    $outcomes->{qc}     = 0;
    $outcomes->{qc_seq} = 0;
    $outcomes->{qc_lib} = undef;
    my $i = $c->get_component(0)->tag_index;
    if ($i == 3) {
      $outcomes->{qc_lib} = 1;
    } elsif ($i == 4) {
      $outcomes->{qc_lib} = 0;
    }
    is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
      "correct outcome for $i when some lanes fail, some lanes pass");    
  }

  for my $lane (@lane_rows) {
    $lane->update({'id_mqc_outcome' => 3});
  }
  $lane_rows[1]->update({'id_mqc_outcome' => 1});
  for my $c (@compositions) {
    my $outcomes = {};
    $outcomes->{qc}     = undef;
    $outcomes->{qc_seq} = undef;
    $outcomes->{qc_lib} = undef;
    my $i = $c->get_component(0)->tag_index;
    if (($i == 3) || ($i == 4)) {
      throws_ok { $mqc->retrieve_outcomes($c)}
        qr/Inconsistent qc outcomes/,
        "error for $i when one lane undef, others pass";
    } else {
      is_deeply ($mqc->retrieve_outcomes($c), $outcomes,
      "correct outcome for $i when one lane undef, others pass");
    }  
  }
};

1;
