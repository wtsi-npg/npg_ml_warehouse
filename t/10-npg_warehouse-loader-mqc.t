use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use Moose::Meta::Class;
use npg_testing::db;

use_ok('npg_warehouse::loader::mqc');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_qc;

my @qc_types = qw/mqc mqc_lib mqc_seq/;

lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]) } 'qc test db created';

 subtest 'object initialization and input checking' => sub {
  plan tests => 8;

  throws_ok {npg_warehouse::loader::mqc->new(schema_qc => $schema_qc) }
    qr/Attribute \(plex_key\) is required /,
    'error if plex_key attr is not set';

  my $mqc;
  lives_ok { $mqc  = npg_warehouse::loader::mqc->new( 
                                             schema_qc => $schema_qc, 
                                             plex_key => 'plex'
                                                   )}
  'object instantiated';
  isa_ok ($mqc, 'npg_warehouse::loader::mqc');
  is ($mqc->verbose, 0, 'verbose mode is off by default');

  throws_ok { $mqc->retrieve_lane_outcomes() }
    qr/Run id is missing/, 'no run id - error';
  throws_ok { $mqc->retrieve_lane_outcomes(0) }
    qr/Run id is missing/, 'zero run id - error';
  throws_ok { $mqc->retrieve_lane_outcomes(1) }
    qr/Position is missing/, 'no position - error';
  throws_ok { $mqc->retrieve_lane_outcomes(1, 0) }
    qr/Position is missing/, 'zero position - error';
};

subtest 'saving data' => sub {
  plan tests => 10;

  my $mqc  = npg_warehouse::loader::mqc->new( 
                                schema_qc => $schema_qc, 
                                plex_key => 'plex'
                                           );
  lives_ok { $mqc->_save_outcomes() } 'no args - nothing to do';

  my $input = {};
  $mqc->_save_outcomes($input);
  is_deeply( $input, {}, 'no column names - nothing saved');

  $mqc->_save_outcomes($input, []);
  is_deeply( $input, {}, 'no column names - nothing saved');

  $mqc->_save_outcomes($input, ['col1']);
  is_deeply( $input, {'col1' => undef}, 'undefined value saved');

  $mqc->_save_outcomes($input, ['col1'], []);
  is_deeply( $input, {'col1' => undef}, 'undefined value saved');

  $mqc->_save_outcomes($input, ['col1'], [], 'value1');
  is_deeply( $input, {'col1' => 'value1'}, 'given value saved');

  $mqc->_save_outcomes($input, ['col1', 'col2'], [], 'value1');
  is_deeply( $input,
    {'col1' => 'value1', 'col2' => 'value1'},
    'given value saved for two columns');
  
  $input = {'col1' => 'value1', 'col2' => 'value1'};
  $mqc->_save_outcomes($input, ['col1'], [], 'value2');
  is_deeply( $input,
    {'col1' => 'value2', 'col2' => 'value1'}, 'given value saved');

  $input = {};
  my @tags = (2 .. 5);
  foreach my $i ( @tags ) {
    foreach my $c (qw/c1 c2 c3/) {
      $input->{'plex'}->{$i}->{$c} = undef;
    }
  }
  my %input_copy = %{$input};
  my $expected = \%input_copy;
  $expected->{'plex'}->{2}->{'c1'} = 1;
  $expected->{'plex'}->{2}->{'c2'} = 1;
  $expected->{'plex'}->{3}->{'c1'} = 1;
  $expected->{'plex'}->{3}->{'c2'} = 1;

  $mqc->_save_outcomes($input, [qw/c1 c2/], [2,3], 1);
  is_deeply( $input, $expected, 'values saved correctly');

  $expected->{'plex'}->{5}->{'c3'} = 1;
  $mqc->_save_outcomes($input, [qw/c3/], [5], 0);
  is_deeply( $input, $expected, 'values saved correctly');    
};

subtest 'retrieve data, seq outcomes only' => sub {
  plan tests => 6;

  my $rs = $schema_qc->resultset('MqcOutcomeEnt');
  for my $r ((3, 33, 333)) {
    for my $p ((1 .. 8)) {
      $rs->create({id_run => $r, position => $p, id_mqc_outcome => 1});
    }
  }
  
  my $srs = $rs->search({id_run => 3});
  while (my $row = $srs->next) {
    my $id = $row->position < 5 ? 3 : 4; # all final, some pass, some fail
    $row->update({id_mqc_outcome => $id, reported => $row->get_time_now()});
  }

  my $expected = {};
  for my $p ((1 .. 8)) {
    for my $t (@qc_types) {
      $expected->{$p}->{$p}->{$t} = undef; 
    }
  }

  my $mqc  = npg_warehouse::loader::mqc->new( 
                                schema_qc => $schema_qc, 
                                plex_key => 'plex'
                                           );

  is_deeply ($mqc->retrieve_lane_outcomes(33, 1, []), $expected->{1},
    'all outcomes undefined');

  my $row = $rs->find({id_run => 33, position => 2});
  $row->update({id_mqc_outcome => 3, reported => $row->get_time_now()});
  my $time = $row->reported();
  is_deeply ($mqc->retrieve_lane_outcomes(33, 1, []), $expected->{1},
    'all outcomes undefined');

  $rs->find({id_run => 33, position => 1})
    ->update({id_mqc_outcome => 4, reported => $time});
  $expected->{1}->{1}->{'mqc'} = 0;
  $expected->{1}->{1}->{'mqc_seq'} = 0;

  is_deeply ($mqc->retrieve_lane_outcomes(33, 1, []), $expected->{1},
    'two fail outcomes defined');

  $expected->{2}->{2}->{'mqc'} = 1;
  $expected->{2}->{2}->{'mqc_seq'} = 1;
  is_deeply ($mqc->retrieve_lane_outcomes(33, 2, []), $expected->{2},
    'two pass outcomes defined');

  my @tags = (3, 5, 7);
  for my $p ((1, 2)) {
    for my $t (@qc_types) {
      delete $expected->{$p}->{$p}->{$t};
      for my $tag (@tags) {
        $expected->{$p}->{$p}->{'plex'}->{$tag}->{$t} =
          ($t eq 'mqc_lib') ? undef : (($p == 1) ? 0 : 1);
      }
    }
  }

  is_deeply ($mqc->retrieve_lane_outcomes(33, 1, \@tags), $expected->{1},
    'two fail outcomes defined for each plex');
  is_deeply ($mqc->retrieve_lane_outcomes(33, 2, \@tags), $expected->{2},
    'two pass outcomes defined for each plex');  
};

subtest 'retrieve data, seq+lib outcomes for a one lib lane' => sub {
  plan tests => 4;

  my $rs = $schema_qc->resultset('MqcLibraryOutcomeEnt');
  $rs->create({id_run => 3, position => 1, id_mqc_outcome => 3}); #final pass
  $rs->create({id_run => 3, position => 2, id_mqc_outcome => 4}); #final fail
  $rs->create({id_run => 3, position => 5, id_mqc_outcome => 5}); #final undefined
  $rs->create({id_run => 3, position => 6, id_mqc_outcome => 1});

  my $expected = {};
  for my $p ((1, 2, 5, 6)) {
    for my $t (@qc_types) {
      $expected->{$p}->{$p}->{$t} = undef; 
    }
  }

  $expected->{1}->{1}->{'mqc_seq'} = 1;
  $expected->{2}->{2}->{'mqc_seq'} = 1;
  $expected->{5}->{5}->{'mqc_seq'} = 0;
  $expected->{6}->{6}->{'mqc_seq'} = 0;

  $expected->{1}->{1}->{'mqc_lib'} = 1;
  $expected->{2}->{2}->{'mqc_lib'} = 0;

  $expected->{1}->{1}->{'mqc'} = 1;
  $expected->{2}->{2}->{'mqc'} = 0;
  $expected->{5}->{5}->{'mqc'} = 0;
  $expected->{6}->{6}->{'mqc'} = 0;

  my $mqc  = npg_warehouse::loader::mqc->new( 
                                schema_qc => $schema_qc, 
                                plex_key => 'plex'
                                           );

  foreach my $p ((1, 2, 5, 6)) {
    is_deeply ($mqc->retrieve_lane_outcomes(3, $p, []), $expected->{$p},
    'correct outcomes');
  }
};

subtest 'retrieve data, seq+lib outcomes for a pool' => sub {
  plan tests => 2;

  my $rs = $schema_qc->resultset('MqcLibraryOutcomeEnt');
  my @tags = (1, 2, 5, 6);

  foreach my $tag (@tags) {
    my $outcome = ($tag < 6) ? 3 : 4;
    $rs->create({  id_run         => 333,
                   position       => 1,
                   tag_index      => $tag,
                   id_mqc_outcome => $outcome,
               });
    $rs->create({ id_run         => 333,
                  position       => 6,
                  tag_index      => $tag,
                  id_mqc_outcome => 6,
               });
  }

  $rs = $schema_qc->resultset('MqcOutcomeEnt');
  $rs->search({id_run => 333, position => 1})
     ->update({id_mqc_outcome => 3}); # lane final pass
  $rs->search({id_run => 333, position => 6}) 
     ->update({id_mqc_outcome => 4}); # lane final fail

  my $expected = {};
  for my $t (@qc_types) {
    for my $tag (@tags) {
      $expected->{1}->{1}->{'plex'}->{$tag}->{$t} = 1;
    }
  }
  $expected->{1}->{1}->{'plex'}->{6}->{'mqc'} = 0;
  $expected->{1}->{1}->{'plex'}->{6}->{'mqc_lib'} = 0;

  for my $tag (@tags) {
    $expected->{6}->{6}->{'plex'}->{$tag}->{'mqc_seq'} = 0;
    $expected->{6}->{6}->{'plex'}->{$tag}->{'mqc'} = 0;
    $expected->{6}->{6}->{'plex'}->{$tag}->{'mqc_lib'} = undef;
  }

  my $mqc  = npg_warehouse::loader::mqc->new( 
                                schema_qc => $schema_qc, 
                                plex_key => 'plex'
                                           );

  foreach my $p ((1, 6)) {
    is_deeply ($mqc->retrieve_lane_outcomes(333, $p, \@tags), $expected->{$p},
    'correct outcomes');
  }
};

1;
