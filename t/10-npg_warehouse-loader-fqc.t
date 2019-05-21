use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use Test::Warn;
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
my $digests = {};

for my $p ((1 .. 6)) {
  my $q = {id_run => 3, position => $p};
  $q->{'id_seq_composition'} =
    t::util::find_or_save_composition($schema_qc, $q);
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  my $row = $rs->create($q);
  $digests->{$row->composition_digest} = $row->composition;
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
  $digests->{$row->composition_digest} = $row->composition;
}

subtest 'object initialization and input checking' => sub {
  plan tests => 4;

  my $mqc;
  lives_ok {$mqc = npg_warehouse::loader::fqc->new(
                   digests => $digests, schema_qc => $schema_qc)}
    'object instantiated';
  isa_ok ($mqc, 'npg_warehouse::loader::fqc');
  throws_ok { $mqc->retrieve_seq_outcome() }
    qr/rpt key is required/, 'no composition digest - error';
  throws_ok { $mqc->retrieve_outcomes() }
    qr/Composition digest is required/, 'no composition digest - error';
};

subtest 'retrieve lane seq outcome' => sub {
  plan tests => 4; 

  my $mqc  = npg_warehouse::loader::fqc->new( 
             digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_seq_outcome('3:1'), {qc_seq => undef},
    'undefined for a non-final outcome');
  is_deeply ($mqc->retrieve_seq_outcome('3:2'), {qc_seq => 1}, '1 for accepted final');
  is_deeply ($mqc->retrieve_seq_outcome('3:5'), {qc_seq => 0}, '0 for rejected final');
  is_deeply ($mqc->retrieve_seq_outcome('3:8'), {qc_seq => undef}, 'undef for no record');  
};

subtest 'retrieve outcomes for a lane' => sub {
  plan tests => 4;

  my $outcomes = {};
  $outcomes->{qc} = undef;
  $outcomes->{qc_seq} = undef;
  $outcomes->{qc_lib} = undef;

  my $mqc = npg_warehouse::loader::fqc->new( 
            digests => $digests, schema_qc => $schema_qc);

  my $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 1)]);
  is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
    'non-final seq outcome, all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 8)]);
  is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
    'no record, all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 2)]);
  $outcomes->{qc_seq} = 1;
  $outcomes->{qc} = 1;
  is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
    'accepted final seq outcome result');

  $outcomes->{qc_seq} = 0;
  $outcomes->{qc} = 0;
  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 5)]);
  is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
    'rejected final seq outcome result'); 
};

subtest 'retrieve outcomes for a one component plex' => sub {
  plan tests => 19;

  my $rsl = $schema_qc->resultset('MqcLibraryOutcomeEnt');

  my $outcomes = {};
  $outcomes->{qc} = undef;
  $outcomes->{qc_seq} = undef;
  $outcomes->{qc_lib} = undef;
  my $q = {};

  my $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 8, tag_index => 1)]);
  my $digest = $c->digest;
  $digests->{$digest} = $c;

  my $mqc = npg_warehouse::loader::fqc->new( 
            digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes, 'no record - all undefined');

  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 8, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
            digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'no record for seq, no final record for lib - all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 7, tag_index => 1)]);
  $digest = $c->digest;
  $digests->{$digest} = $c;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes, 'no record - all undefined');

  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 7, tag_index => 1});
  $q->{'id_mqc_outcome'} = 3; #'Accepted final' 
  $rsl->create($q);
  $outcomes->{qc_lib} = 1;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes, 'lib final pass, lane - no record - overall undefined');
  
  $outcomes->{qc_lib} = undef;
  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 1, tag_index => 1)]);
  $digest = $c->digest;
  $digests->{$digest} = $c;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'no lib outcome, non-final seq outcome - all undefined');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 3, tag_index => 1)]);
  $digest = $c->digest;
  $digests->{$digest} = $c;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'no lib outcome, non-final seq outcome - all undefined');

  $q = {};
  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 3, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  my $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'both lib and seq outcomes non-final - all undefined');

  $o->delete();
  $q->{'id_mqc_outcome'} = 3; #'Accepted final'
  $rsl->create($q);
  $outcomes->{qc_lib} = 1;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'lib final, seq outcomes non-final - overall undefined');

  $outcomes->{qc} = undef;
  $outcomes->{qc_seq} = undef;
  $outcomes->{qc_lib} = undef;
  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 5, tag_index => 1)]);
  $digest = $c->digest;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'all undefined - digest not cached');

  $digests->{$digest} = $c;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_seq} = 0;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'final fail seq outcome, no lib outcome - overall undefined');

  $q = {};
  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 5, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq outcome final, lib not final - overall undefined');

  $o->delete();
  $q->{'id_mqc_outcome'} = 3; #'Accepted final';
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_lib} = 1;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq and lib outcomes final, but opposite - overall fail');

  $o->delete();
  $q->{'id_mqc_outcome'} = 4; #'Rejected final';
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_lib} = 0;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq and lib outcomes final all fail - all fail');

  $o->delete();
  $q->{'id_mqc_outcome'} = 6; #'Undecided final';
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_lib} = undef;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq and lib outcomes final, lib undef - overall fail');

  $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => 3, position => 2, tag_index => 1)]);
  $digest = $c->digest;
  $digests->{$digest} = $c;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_seq} = 1;
  $outcomes->{qc} = 1;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'final seq outcome, no lib outcome - overall pass');

  $q = {};
  $q->{'id_seq_composition'} = t::util::find_or_save_composition(
        $schema_qc, {id_run => 3, position => 2, tag_index => 1});
  $q->{'id_mqc_outcome'} = 1; #'Accepted preliminary' 
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq outcome pass final, lib not final - overall pass');

  $o->delete();
  $q->{'id_mqc_outcome'} = 3; #'Accepted final';
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_lib} = 1;
  $outcomes->{qc} = 1;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq and lib outcomes final - all pass');

  $o->delete();
  $q->{'id_mqc_outcome'} = 4; #'Rejected final';
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_lib} = 0;
  $outcomes->{qc} = 0;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq and lib outcomes final, lib fail - lib and qc fail');

  $o->delete();
  $q->{'id_mqc_outcome'} = 6; #'Undecided final';
  $o = $rsl->create($q);
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  $outcomes->{qc_lib} = undef;
  $outcomes->{qc} = 1;
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
    'seq and lib outcomes final, lib undef, qc pass');
};

subtest 'retrieve outcomes for a multi-component plex' => sub {
  plan tests => 99;

  my $id_run = 4;
  $digests = {};
  my @lane_rows = ();
  for my $p ((1 .. 4)) {
    my $q = {id_run => $id_run, position => $p};
    $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, $q);
    $q->{'id_mqc_outcome'} = 3; #'Accepted final'
    my $row = $rs->create($q);
    push @lane_rows, $row;
    $digests->{$row->composition_digest} = $row->composition;
  }

  my $rsl = $schema_qc->resultset('MqcLibraryOutcomeEnt');
  my @compositions = ();
  my @compositions_unmerged;
  for my $i ((1 .. 6)) {
    my @queries =
      map { {id_run => $id_run, position => $_, tag_index => $i} }
      (1 .. 4);
    my $q = {};
    $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, @queries);
    $q->{'id_mqc_outcome'} = $i; 
    my $row = $rsl->create($q);
    my $composition = $row->composition;
    push @compositions, $composition;
    $digests->{$row->composition_digest} = $composition;

    my @single_c_compositions = 
      map { npg_tracking::glossary::composition->new(components=>[$_]) }
      $composition->components_list();
    for my $ssc (@single_c_compositions) {
      $digests->{$ssc->digest} = $ssc;
    }
    push @compositions_unmerged, @single_c_compositions; 
  }

  my $mqc  = npg_warehouse::loader::fqc->new( 
             digests => $digests, schema_qc => $schema_qc);

  for my $c (@compositions, @compositions_unmerged) {
    my $outcomes = {};
    $outcomes->{qc}     = 1;
    $outcomes->{qc_seq} = 1;
    $outcomes->{qc_lib} = undef;
    my $i = $c->get_component(0)->tag_index;
    if ($i == 3) {
      $outcomes->{qc_lib} = 1;
    } elsif ($i == 4) {
      $outcomes->{qc}     = 0;
      $outcomes->{qc_lib} = 0;
    }
    is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
      "correct outcome for plex $i when all lanes pass");    
  }

  for my $lane (@lane_rows) {
    $lane->update({'id_mqc_outcome' => 4});
  }
  $mqc  = npg_warehouse::loader::fqc->new( 
          digests => $digests, schema_qc => $schema_qc);

  for my $c (@compositions, @compositions_unmerged) {
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
    is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
      "correct outcome for plex $i when all lanes fail");    
  }

  $lane_rows[0]->update({'id_mqc_outcome' => 3});
  $mqc  = npg_warehouse::loader::fqc->new( 
          digests => $digests, schema_qc => $schema_qc);

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
    if ($c->num_components == 1 && $c->get_component(0)->position == 1) {
      $outcomes->{qc_seq} = 1;
      if (defined $outcomes->{qc_lib} && $outcomes->{qc_lib} == 0) {
        $outcomes->{qc} = 0;
      } else {
        $outcomes->{qc} = 1;
      }
    }
    is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
      "correct outcome for $i when some lanes fail, some lanes pass");    
  }

  for my $lane (@lane_rows) {
    $lane->update({'id_mqc_outcome' => 3});
  }
  $lane_rows[1]->update({'id_mqc_outcome' => 1});
  $mqc  = npg_warehouse::loader::fqc->new( 
          digests => $digests, schema_qc => $schema_qc);

  for my $c (@compositions, @compositions_unmerged) {
    my $outcomes = {};
    my $i = $c->get_component(0)->tag_index;
    $outcomes->{qc}     = undef;
    $outcomes->{qc_seq} = undef;
    $outcomes->{qc_lib} = ($i == 3) ? 1 : (($i == 4) ? 0 : undef);

    if ($c->num_components == 1) {
      if ($c->get_component(0)->position == 2) {
        $outcomes->{qc_seq} = undef;
        $outcomes->{qc}     = undef;
      } else {
        $outcomes->{qc_seq} = 1;
        $outcomes->{qc} = ($i == 4) ? 0 : 1;
      }
    }

    is_deeply ($mqc->retrieve_outcomes($c->digest), $outcomes,
      "correct outcome for $i when one lane undef, others pass"); 
  }

  my $q = {id_run => $id_run, position => 1, tag_index => 4};
  $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, $q);
  $q->{'id_mqc_outcome'} = 3; #'Accepted final';
  my $row = $rsl->create($q);
  my $digest = $row->composition_digest;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  my $outcomes = {qc_seq => 1, qc_lib => 1, qc => 1};
  is_deeply ($mqc->retrieve_outcomes($digest), $outcomes,
      'existing lib value is not overwritten');

  $q = {};
  my @qs = ({id_run => $id_run, position => 1, tag_index => 3},
            {id_run => $id_run, position => 3, tag_index => 3});
  $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, @qs);
  $q->{'id_mqc_outcome'} = 4; #'Regected final';
  $row = $rsl->create($q);
  $digest = $row->composition_digest;
  $digests->{$digest} = $row->composition;
  $mqc = npg_warehouse::loader::fqc->new( 
         digests => $digests, schema_qc => $schema_qc);
  my $retrieved;
  warnings_like {$retrieved = $mqc->retrieve_outcomes($digest)}
    [qr/Conflicting inferred outcomes for/, qr/Conflicting inferred outcomes for/],
    'warning about conflicting outcome values';
  $outcomes = {qc_seq => 1, qc_lib => 0, qc => 0};
  is_deeply ($retrieved, $outcomes,
    'lib and overall outcomes are a fail when inferred lib outcomes are in conflict');
};

1;
