use strict;
use warnings;
use Test::More tests => 20;
use Test::Exception;
use Test::Warn;
use Test::Deep;
use Moose::Meta::Class;
use File::Copy::Recursive qw/dircopy fcopy/;
use File::Temp qw/tempdir/;

use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use npg_tracking::glossary::rpt;
use npg_testing::db;
use npg_qc::autoqc::qc_store;
use t::util;

my $compos_pkg = 'npg_tracking::glossary::composition';
my $compon_pkg = 'npg_tracking::glossary::composition::component::illumina';

my $RUN_LANE_TABLE_NAME      = q[IseqRunLaneMetric];
my $PRODUCT_TABLE_NAME       = q[IseqProductMetric];
my $PRODUCTC_TABLE_NAME      = q[IseqProductComponent];
my $LIMS_FK_COLUMN_NAME      = q[id_iseq_flowcell_tmp];
my @basic_run_lane_columns = qw/cycles
                                paired_read
                                cancelled
                                run_priority
                                instrument_name
                                instrument_model
                                raw_cluster_density
                                pf_cluster_density
                                q30_yield_kb_forward_read
                                q30_yield_kb_reverse_read
                                q40_yield_kb_forward_read
                                q40_yield_kb_reverse_read/;

 my @columns = qw/tags_decode_percent
                  tags_decode_cv/;

use_ok('npg_warehouse::loader::run');
throws_ok {npg_warehouse::loader::run->new()}
    qr/Attribute \(id_run\) is required/,
    'error in constructor when id_run attr is not defined';

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my ($schema_npg, $schema_qc, $schema_wh);

lives_ok{ $schema_wh  = $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema],
  q[t/data/fixtures/wh]) } 'warehouse test db created';
lives_ok{ $schema_npg  = $util->create_test_db(q[npg_tracking::Schema],
  q[t/data/fixtures/npg]) } 'npg test db created';
lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]) } 'npgqc test db created';

my $autoqc_store =  npg_qc::autoqc::qc_store->new(use_db => 0);

my $folder_glob = q[t/data/runfolders/];
my $user_id = 7;

my $init = { _autoqc_store => $autoqc_store,
             schema_npg   => $schema_npg, 
             schema_qc    => $schema_qc, 
             schema_wh    => $schema_wh,
             verbose       => 0,
             explain       => 0,
           };

################################################################
#         Test cases description
################################################################
#batch_id # id_run # paired_id_run # paired_read # wh # npg # qc
################################################################
#2044     #  1272   # 1246          # 1           # 1  #  1  # 1
#4354     #  3500   # 3529          # 1           # 1  #  1  # 1
#4178     #  3323   # 3351          # 1           # 1  #  1  # 1
#4445     #  3622   #               # 0           # 1  #  1  # 1
#4915     #  3965   #               # 1           # 1  #  1  # 1
#4965     #  4025   #               # 1           # 1  #  1  # 1
#4380     #  3519   #               #             #    #  1  #
#5169     #  4138   #               #             #    #  1  #  this run is cancelled without qc complete status
#5498     #  4333   #               # 1           #    #  1  # 1 tag decoding stats added
#6669     #  4779   # 
#12509    #  6624   #               # 1           #    #     # 1 split and bam stats added; tag metrics and tag decode added; pulldown metrics added
#12498    #  6642   #               # 1           #    #     # 1 split and bam stats added
################################################################

subtest 'old paired (two runfolders) run' => sub {
  plan tests => 31;

  my %in = %{$init};
  $in{'id_run'} = 1246;
  delete $in{'_autoqc_store'};
  my $loader;

  lives_ok {$loader  = npg_warehouse::loader::run->new(\%in)}
    'loader object instantiated by passing schema objects to the constructor';
  isa_ok ($loader, 'npg_warehouse::loader::run');
  ok (!$loader->_old_forward_id_run, 'old forward id run is not set');
  $loader->load();
  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 1246});
  is ($rs->count, 8, '8 run-lane rows for run 1246');
  
  $in{'id_run'} = 1272;
  $in{'verbose'} = 1;
  my $loader1 = npg_warehouse::loader::run->new(\%in);
  is ($loader1->_old_forward_id_run, 1246, 'old forward id run is set');
  warning_like { $loader1->load() }
    qr/Run 1272 is an old reverse run for 1246, not loading/,
    'warning about not loading an old reverse run';
  is ($schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
    {id_run => 1272,},)->count, 0, 'no rows for an old reverse run 1272');

  my $r  = $rs->next;
  is ($r->position, 1, 'position from a result set for position 1');
  is ($r->qc_complete->datetime, '2008-09-25T13:18:20', 'run complete for position 1');
  is ($r->run_pending->datetime, '2008-08-19T09:55:12', 'run pending for position 1');

  my @found = ();
  my @expected = (37,1,0,1,'IL20','1G',undef,undef,3,0,4,0);
  foreach my $column (@basic_run_lane_columns) {
    push @found, $r->$column;
  }
  is_deeply(\@found, \@expected, 'run-lane data loaded correctly');

  $r = $rs->next;
  is ($r->position, 2, 'result set for position 2');
  is ($r->run_pending->datetime, '2008-08-19T09:55:12', 'run pending for position 2');
  is ($r->qc_complete->datetime, '2008-09-25T13:18:20', 'run complete for position 2');

  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => 1246,});
  is ($rs->count, 1, '1 product row for run 1246');
  $r = $rs->next;
  is ($r->position, 1, 'position correct');
  is ($r->q30_yield_kb_forward_read, 3, 'forward read q30 for the product');
  is ($r->q40_yield_kb_forward_read, 4, 'forward read q40 for the product');
  ok (!$r->$LIMS_FK_COLUMN_NAME, 'lims fk not set');
  is ($r->qc, undef, 'qc value undefined');
  is ($r->qc_seq, undef, 'seq qc value undefined');
  is ($r->qc_lib, undef, 'lib qc value undefined');

  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({batch_id => undef, flowcell_id => undef, id_run => 1246, })}
    'both batch and flowcell ids unset - test prerequisite';
  %in = %{$init};
  delete $in{'_autoqc_store'};
  $in{'id_run'}  = 1246;
  $in{'explain'} = 1;
  lives_ok {$loader  = npg_warehouse::loader::run->new(\%in)}
    'loader object instantiated by passing schema objects to the constructor';
  warning_like { $loader->_flowcell_table_fks } 
    qr/Tracking database has no flowcell information for run 1246/,
    'warning about absence of lims data in tracking db';
  lives_ok { $loader->load() } 'absence of lims data does not lead to an error';

  for my $p ((1 .. 3)) {
    my $q = {id_run => 1246, position => $p};
    $q->{'id_seq_composition'} = t::util::find_or_save_composition($schema_qc, $q);
    $q->{'id_mqc_outcome'} = 3;
    $schema_qc->resultset('MqcOutcomeEnt')->create($q);
    $q = {id_run => 1246, position => $p, tag_index => 8};
    $q->{'id_seq_composition'} = t::util::find_or_save_composition($schema_qc, $q);
    $q->{'id_mqc_outcome'} = 3;
    $schema_qc->resultset('MqcLibraryOutcomeEnt')->create($q);
  }
  
  npg_warehouse::loader::run->new(\%in)->load();
  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => 1246,});
  
  is ($rs->count, 1, '1 product row for run 1246');
  $r = $rs->next;
  is ($r->position, 1, 'position correct');
  is ($r->qc, 1, 'qc value 1');
  is ($r->qc_seq, 1, 'seq qc value 1');
  is ($r->qc_lib, undef, 'lib qc value undefined');  
};

subtest 'old paired (two runfolders) run' => sub {
  plan tests => 10;

  my %in = %{$init};
  $in{'id_run'} = 4138;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();
  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 4138,},);
  is ($rs->count, 8,'8 rows for run 4138');

  $in{'id_run'} = 3965;
  $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();
  my $r = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 3965,position=>1},)->next;
  is ($r->paired_read, 1, 'paired read flag updated correctly');
  is ($r->tags_decode_percent, undef, 'tags_decode_percent NULL where not loaded');
  is ($r->instrument_name, q[IL36] , 'instr name');
  is ($r->instrument_external_name, q[PQKLP], 'instr name given by the manufacturer');
  is ($r->instrument_model, q[HK] , 'instr model');
  $r = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 3965,position=>2},)->next;
  is ($r->instrument_name, q[IL36] , 'instr name');
  is ($r->instrument_external_name, q[PQKLP], 'instr name given by the manufacturer');
  is ($r->instrument_model, q[HK] , 'instr model');

  $in{'id_run'} = 3323;
  $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();
  $r = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 3323,position=>1},)->next;
  is($r->pf_cluster_density, undef, 'pf_cluster_density undefined'); 
};

subtest 'indexed run' => sub {
  plan tests => 45;

  my $id_run = 4333;
 
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '100330_IL21_4333', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();

  my $values = {
          1 => {'raw_cluster_density' => 95465.880,  'pf_cluster_density' => 11496.220, 'q30_yield_kb_reverse_read' => '105906', 'q30_yield_kb_forward_read' => '98073', 'q40_yield_kb_forward_read' => '0', 'unexpected_tags_percent' => 0.01},
          2 => {'raw_cluster_density' => 325143.800, 'pf_cluster_density' => 82325.490, 'q30_yield_kb_reverse_read' => '1003112','q30_yield_kb_forward_read' => '563558'},
          3 => {'raw_cluster_density' => 335626.700, 'pf_cluster_density' => 171361.900,'q30_yield_kb_reverse_read' => '1011728','q30_yield_kb_forward_read' => '981688'},
          4 => {'raw_cluster_density' => 175608.400, 'pf_cluster_density' => 161077.600,'q30_yield_kb_reverse_read' => '714510', 'q30_yield_kb_forward_read' => '745267', 'q40_yield_kb_forward_read' => '56', 'q40_yield_kb_reverse_read' => '37'},
          5 => {'raw_cluster_density' => 443386.900, 'pf_cluster_density' => 380473.100,'q30_yield_kb_reverse_read' => '1523282','q30_yield_kb_forward_read' => '1670331'},
          6 => {'raw_cluster_density' => 454826.200, 'pf_cluster_density' => 397424.100,'q30_yield_kb_reverse_read' => '1530965','q30_yield_kb_forward_read' => '1689674'},
          7 => {'raw_cluster_density' => 611192.000, 'pf_cluster_density' => 465809.300,'q30_yield_kb_reverse_read' => '997068', 'q30_yield_kb_forward_read' => '1668517'},
          8 => {'raw_cluster_density' => 511924.700, 'pf_cluster_density' => 377133.300, 'q30_yield_kb_forward_read' => '1111015'},
               };

  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 4333},);

  is ($rs->count, 8, '8 rows loaded for run 4333');
  my $row;
  while ($row = $rs->next) {
    my $position = $row->position;
    foreach my $column (
        qw/raw_cluster_density pf_cluster_density q30_yield_kb_forward_read q30_yield_kb_reverse_read unexpected_tags_percent/) {
      is($row->$column, $values->{$position}->{$column}, qq[$column value for run 4333 position $position]);

    }
  }

  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => 4333,});
  is ($rs->count, 68, '68 product rows for run 4333');

  my @positions = qw/1 4 8/;
  $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
       {id_run => $id_run, position => \@positions},
  );

  my $expected = {};
  foreach my $position (@positions) {
    foreach my $column (@columns) {
      $expected->{$id_run}->{$position}->{$column} = undef;
    }
  }

  $expected->{4333}->{4}->{tags_decode_percent} = undef;
  $expected->{4333}->{4}->{tags_decode_cv} = undef;
  $expected->{4333}->{1}->{tags_decode_percent} = 99.29;
  $expected->{4333}->{1}->{tags_decode_cv} = 55.1;
  $expected->{4333}->{8}->{tags_decode_percent} =81.94;
  $expected->{4333}->{8}->{tags_decode_cv} =122.4;
 
  my $autoqc = {};
  while (my $row = $rs->next) {
    foreach my $column (@columns) { 
      $autoqc->{4333}->{$row->position}->{$column} = $row->$column;
    }
  }
  cmp_deeply($autoqc, $expected, 'loaded autoqc results');
};

subtest 'indexed run' => sub {
  plan tests => 17;

  my $id_run = 4799;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '100330_HS21_4799', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();

  my @rows = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
       {id_run => $id_run, position => [5-8]},
  )->all();

  my $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => $id_run, tag_index => [1,2,3,4]},
  );
  is ($rs->count, 20, '20 rows in the product table for run 4799 for plexes[1-4]');

  my $expected_tag_info =  {
    1 => {tag_decode_percent=>12.99, tag_sequence=>'ATCACGT',},
    2 => {tag_decode_percent=>12.80, tag_sequence=>'CGATGTT',},
    3 => {tag_decode_percent=>4.78,  tag_sequence=>'TTAGGCA',},
    4 => {tag_decode_percent=>10.12, tag_sequence=>'TGACCAC',},
  };

  my $tag_info = {};
  while (my $r = $rs->next) {
    if ($r->position == 7) {
      my $index = $r->tag_index;
      if (defined $index) {
        $tag_info->{$index}->{tag_decode_percent} = $r->tag_decode_percent;
        $tag_info->{$index}->{tag_sequence} = $r->tag_sequence4deplexing;
      }
    }
  }
  cmp_deeply($tag_info, $expected_tag_info, 'tag info for position 7 plexex 1-4');

  my $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => 4799, position=>7, tag_index => 5},
  )->first;
  ok ($r, 'a row for a tag index that is not listed in lims exists');
  is ($r->$LIMS_FK_COLUMN_NAME, undef, 'lims foreign key not defined');

  $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => $id_run, position => 3, tag_index=>1},)->first;
  is ($r->tag_decode_percent, 11.4, 'tag decode percent for tag 1');

  $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => $id_run, position => 3, tag_index=>4},)->first;
  is ($r->insert_size_quartile1, undef, 'quartile undefined for tag 4');

  $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => $id_run, position => 3, tag_index=>0},)->first;
  is ($r->tag_decode_percent, undef, 'tag decode percent undefined for tag 0');
  is ($r->insert_size_quartile3, 207, 'quartile3 correct for tag 0');
  is ($r->q20_yield_kb_forward_read, 46671, 'qx forward lane 3, tag 0');
  is ($r->q20_yield_kb_reverse_read, 39877, 'qx reverse lane 3, tag 0');

  $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => $id_run, position => 3, tag_index=>2},)->first;
  is ($r->insert_size_median, 189, 'median correct for tag 2');

  $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => 4799, position => 3, tag_index=>3},)->first;
  is ($r->q20_yield_kb_forward_read, 1455655, 'qx forward lane 3, tag 3');
  is ($r->q20_yield_kb_reverse_read, 1393269, 'qx reverse lane 3, tag 3');

  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => $id_run, tag_index => undef,},
       {order_by => 'position',},
  );
  is ($rs->count, 2, '2 product rows');
  
  my @acolumns = qw/ q20_yield_kb_forward_read q20_yield_kb_reverse_read /;
  my $found = {}; 
  while (my $row = $rs->next) {
    foreach my $column (@acolumns) {
      $found->{$row->position}->{$column} = $row->$column;
    }
  }
  my $e = {};
  $e->{1}->{q20_yield_kb_forward_read} = 46671;
  $e->{1}->{q20_yield_kb_reverse_read} = 39877;
  $e->{4}->{q20_yield_kb_forward_read} = 1455655;
  $e->{4}->{q20_yield_kb_reverse_read} = 1393269;
 
  is_deeply ($found, $e, 'lane product autoqc results');  
};

subtest 'indexed run' => sub {
  plan tests => 38;

  my $id_run = 6624;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '110731_HS17_06624_A_B00T5ACXX', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

  my $outcomes = {'6624:1'   => 3,
                  '6624:2'   => 4,
                  '6624:2:1' => 5,
                  '6624:3'   => 3,
                  '6624:3:1' => 3,
                  '6624:3:3' => 4,
                  '6624:3:4' => 4
                 }; 
  for my $rpt (keys %{$outcomes}) {
    my $q = npg_tracking::glossary::rpt->inflate_rpt($rpt);
    $q->{'id_seq_composition'} = t::util::find_or_save_composition($schema_qc, $q);
    $q->{'id_mqc_outcome'}     = $outcomes->{$rpt};
    my $rs_name = defined $q->{'tag_index'} ? 'MqcLibraryOutcomeEnt' : 'MqcOutcomeEnt';
    $schema_qc->resultset($rs_name)->create($q);
  }

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();

  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => $id_run},);
  is($rs->count, 8, '8 rows in run-lane table');

  my $lane = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->find({id_run=>$id_run,position=>2});
  is($lane->q30_yield_kb_reverse_read, 9820023, 'q30 lane reverse');
  is($lane->q40_yield_kb_forward_read, 6887095, 'q40 lane forward');
  cmp_ok(sprintf('%.2f',$lane->tags_decode_percent), q(==), 98.96,
    'lane 2 tag decode percent from tag metrics in presence of tag decode stats'); 
  
  $lane = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->find({id_run=>$id_run,position=>3});
  cmp_ok(sprintf('%.2f',$lane->tags_decode_percent()), q(==), 99.05,
    'lane 3 tag decode percent from tag decode stats in absence of tag metrics');

  $lane = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->find({id_run=>$id_run,position=>4});
  is($lane->q30_yield_kb_reverse_read, 11820778, 'q30 lane reverse');
  is($lane->q40_yield_kb_forward_read, 8315876,  'q40 lane forward');

  my $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->find({id_run=>6624,position=>1,tag_index=>0});
  ok(!defined $plex->tag_sequence4deplexing(), 'index zero tag sequence is not defined');
  is($plex->tag_decode_count(), 1831358, 'lane 1 tag index 0 count');
  is($plex->qc, 1, 'qc value pass');
  is($plex->qc_lib, undef, 'qc lib value undefined');
  is($plex->qc_seq, 1, 'qc seq value pass');

  is ($schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>2,tag_index=>undef})->count,
    0, 'lane 2 is not in product table');
  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->find({id_run=>$id_run,position=>2,tag_index=>168});
  is($plex->q30_yield_kb_reverse_read, 304, 'q30 plex reverse');
  is($plex->q40_yield_kb_forward_read, 210, 'q40 plex forward');
  is($plex->tag_sequence4deplexing(), 'ACAACGCA', 'lane 2 tag index 168 tag sequence');
  is($plex->tag_decode_count(), 1277701, 'lane 2 tag index 168 count');
  cmp_ok(sprintf('%.2f', $plex->tag_decode_percent()), q(==), 0.73, ,
    'lane 2 tag index 168 percent');
  is($plex->qc, 0, 'qc value fail');
  is($plex->qc_lib, undef, 'qc lib value undefined');
  is($plex->qc_seq, 0, 'qc seq fail');

  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>3,tag_index=>1})->first;
  cmp_ok(sprintf('%.2f',$plex->mean_bait_coverage()), q(==), 41.49, 'mean bait coverage');
  cmp_ok(sprintf('%.2f',$plex->on_bait_percent()), q(==), 68.06, 'on bait percent');
  cmp_ok(sprintf('%.2f',$plex->on_or_near_bait_percent()), q(==), 88.92, 'on or near bait percent');
  is($plex->qc, 1, 'qc value 1');
  is($plex->qc_lib, 1, 'qc lib value undefined');
  is($plex->qc_seq, 1, 'qc seq value 1');

  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>3,tag_index=>4})->first;
  cmp_ok(sprintf('%.2f',$plex->num_reads()), q(==), 33605036, 'bam number of reads');
  cmp_ok(sprintf('%.2f',$plex->percent_mapped()), q(==), 96.12, 'bam (nonphix) mapped percent');
  cmp_ok(sprintf('%.2f',$plex->percent_duplicate()), q(==), 1.04, 'bam (nonphix) duplicate percent');
  is($plex->qc, 0, 'qc value 0');
  is($plex->qc_lib, 0, 'qc lib value undefined');
  is($plex->qc_seq, 1, 'qc seq value 1');

  is ($schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>4,tag_index=>undef})->count,
    0, 'lane 4 is not in product table');
  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->find({id_run=>$id_run,position=>4,tag_index=>0});
  is($plex->q30_yield_kb_reverse_read, 99353, 'q30 plex reverse');
  is($plex->q40_yield_kb_forward_read, 72788, 'q40 plex forward');
};

subtest 'indexed run' => sub {
  plan tests => 23;

  my $id_run = 6642;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '110804_HS22_06642_A_B020JACXX', id_run => $id_run, })}
    'forder glob reset for run 6642 lives - test prerequisite';

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();

  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => $id_run},);
  is($rs->count, 8, '8 rows in run-lane table');

  my $lane = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>1,tag_index=>undef})->first;
  ok ($lane, 'product row for lane 1 is present');
  my $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1)])->digest;
  is ($lane->id_iseq_product, $d, 'id product');
  $lane = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>2,tag_index=>undef})->first;
  ok (!$lane, 'product row for lane 2 is not present');

  $lane = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => $id_run, position=>3,tag_index=>undef},)->first;
  ok ($lane, 'product row for lane 3 is present');
  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 3)])->digest;
  is ($lane->id_iseq_product, $d, 'id product');
  cmp_ok(sprintf('%.2f',$lane->num_reads()), q(==), 308368522, 'bam number of reads');
  cmp_ok(sprintf('%.2f',$lane->percent_mapped()), q(==), 98.19, 'bam mapped percent');
  cmp_ok(sprintf('%.2f',$lane->percent_duplicate()), q(==), 24.63, 'bam duplicate percent');
  is ($lane->chimeric_reads_percent, 0.26, 'chimeric reads');

  $lane = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => $id_run, position=>4,tag_index=>undef},)->first;
  ok ($lane, 'product row for lane 4 is present');
  cmp_ok(sprintf('%.5f',$lane->verify_bam_id_score()), q(==), 0.00166, 'verify_bam_id_score');
  cmp_ok(sprintf('%.2f',$lane->verify_bam_id_average_depth()), q(==), 9.42, 'verify_bam_id_average_depth');
  cmp_ok($lane->verify_bam_id_snp_count(), q(==), 1531960, 'verify_bam_id_snp_count');

  my $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>2,tag_index=>4})->first;
  ok ($plex, 'plex row for lane 2 tag index 4 is present');
  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 2, tag_index => 4)])->digest;
  is ($plex->id_iseq_product, $d, 'id product');
  cmp_ok(sprintf('%.2f',$plex->human_percent_mapped()), q(==), 55.3, 'bam human mapped percent');
  cmp_ok(sprintf('%.2f',$plex->human_percent_duplicate()), q(==), 68.09, 'bam human duplicate percent');
  cmp_ok(sprintf('%.2f',$plex->num_reads()), q(==), 138756624, 'bam (nonhuman) number of reads');
  cmp_ok(sprintf('%.2f',$plex->percent_mapped()), q(==), 96.3, 'bam (nonhuman) mapped percent');
  cmp_ok(sprintf('%.2f',$plex->percent_duplicate()), q(==), 6.34, 'bam (nonhuman) duplicate percent');
};

subtest 'linking to lims data - test 1' => sub {
  plan tests => 44;

  $schema_wh->resultset('IseqFlowcell')->find({id_flowcell_lims=>14178, position=>6, tag_index=>168})
   ->update({entity_type => 'library_indexed'});
  is ($schema_wh->resultset('IseqFlowcell')->find({id_flowcell_lims=>14178, position=>6, tag_index=>168})->entity_type,
      'library_indexed',
      'lane 6: set spiked phix as usual indexed library - test prerequisite');
  my $id_run = 6998;
  my %in = %{$init};
  $in{'id_run'} = $id_run;
  $in{'_autoqc_store'} = npg_qc::autoqc::qc_store->new(
    use_db => 1, qc_schema => $schema_qc, verbose => 0);
  $in{'lims_fk_repair'} = 1;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  is ($loader->id_flowcell_lims, 14178, 'id_flowcell_lims populated correctly');

  $loader->load();

  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => $id_run},);
  is($rs->count, 8, '8 rows in run-lane table');

  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => $id_run},);
  is ($rs->count, 30, '30 rows in product table');
  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => $id_run,tag_index=>undef},);
  is ( join(q[ ], sort map {$_->position} $rs->all), '2 6', 'lane-level rows for lane 2 and 6');
  
  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
    {'me.id_run' => $id_run,'me.position' => 2,}, {prefetch => 'iseq_flowcell'});
  is ($rs->count, 1, 'one product record for lane 2');
  my $row = $rs->next;
  is ($row->id_iseq_flowcell_tmp, 93508, 'flowcell fk set');
  my $fc = $row->iseq_flowcell;
  is (ref $fc, 'WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell', 'retrieved flowcell row');
  is ($fc->pipeline_id_lims, 'No PCR', 'Sequencescape library type');
  is ($fc->id_lims, 'SQSCP', 'this is Sequencescape record');
  is ($fc->id_flowcell_lims, 14178, 'batch id correct');
  is ($fc->position, 2, 'position correct');
  is ($fc->tag_index, 154, 'lane data linked to the only target library');
  is ($fc->entity_type, 'library_indexed', 'non-spiked library');
  
  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => $id_run,position => 6});
  is ($rs->count, 1, 'one product record for lane 6');
  ok (!defined $rs->next->id_iseq_flowcell_tmp,
    'flowcell fk not set since the flowcell table reports two none-spiked libraries');
  
  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
        {id_run   => $id_run, position => 3},
        {order_by => 'tag_index'});
  is ($rs->count, 3, 'three product records for lane 3');
  $row = $rs->next;
  is ($row->tag_index, 0, 'tag zero row present');
  ok (!defined $row->id_iseq_flowcell_tmp, 'row not linked to the flowcell table');
  $row = $rs->next;
  is ($row->tag_index, 153, 'tag 153 row present');
  ok ($row->id_iseq_flowcell_tmp, 'row is linked to the flowcell table');
  $fc = $row->iseq_flowcell;
  is (ref $fc, 'WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell', 'retrieved flowcell row');
  is ($fc->position, 3, 'position correct');
  is ($fc->tag_index, 153, 'tag_index correct');  
  is ($fc->entity_type, 'library_indexed', 'non-spiked library');
  ok ($fc->is_spiked, 'library is spiked');
  $row = $rs->next;
  is ($row->tag_index, 168, 'tag 168 row present');
  ok ($row->id_iseq_flowcell_tmp, 'row is linked to the flowcell table');
  $fc = $row->iseq_flowcell;
  is (ref $fc, 'WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell', 'retrieved flowcell row');
  is ($fc->position, 3, 'position correct');
  is ($fc->tag_index, 168, 'tag_index correct');
  is ($fc->entity_type, 'library_indexed_spike', 'this is a spike');
  ok ($fc->is_spiked, 'library is not spiked');

  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
        {id_run   => $id_run, position => 4},
        {order_by => 'tag_index'});
  is ($rs->count, 3, 'three product records for lane 4');
  $row = $rs->next;
  is ($row->tag_index, 0, 'tag zero row present');
  ok (!defined $row->id_iseq_flowcell_tmp, 'row not linked to the flowcell table');
  $row = $rs->next;
  is ($row->tag_index, 152, 'tag 152 row present');
  ok ($row->id_iseq_flowcell_tmp, 'row is linked to the flowcell table');
  $row = $rs->next;
  is ($row->tag_index, 888, 'tag 888 row present - test prerequisite');
  ok ($row->id_iseq_flowcell_tmp, 'row is linked to the flowcell table');
  $fc = $row->iseq_flowcell;
  is (ref $fc, 'WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell', 'retrieved flowcell row');
  is ($fc->position, 4, 'position correct');
  is ($fc->tag_index, 168, 'tag_index correct');
  is ($fc->entity_type, 'library_indexed_spike', 'this is a spike');
};

subtest 'linking to lims data - test 2' => sub {
  plan tests => 29;

  my $id_run = 4486;
  my %in = %{$init};
  $in{'id_run'} = $id_run;
  $in{'_autoqc_store'} = npg_qc::autoqc::qc_store->new(use_db => 1, qc_schema => $schema_qc, verbose => 0);
  $in{'lims_fk_repair'} = 1;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  warnings_exist { $loader->load() }
    [qr/Run 4486: multiple flowcell table records for library, pt key 1/],
    'warning about duplicate entries';
  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => $id_run},);
  is($rs->count, 8, '8 rows in run-lane table');

  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
    {id_run => $id_run}, {order_by => 'position'});
  is ($rs->count, 8, '8 rows in product table');
  my @rows = $rs->all();
  is (scalar(grep {defined $_->tag_index} @rows), 0, 'none of the rows has tag_index set');
  is (scalar(grep {defined $_->id_iseq_flowcell_tmp} @rows), 6, 'six rows are linked to the flowcell table');
  is (join(q[ ], map {$_->position} @rows), '1 2 3 4 5 6 7 8', 'all lanes are represented');

  my $row = $rows[3];
  is ($row->position, 4, 'control lane present');
  my $fc = $row->iseq_flowcell;
  is (ref $fc, 'WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell', 'retrieved flowcell row');
  is ($fc->id_flowcell_lims, 5992, 'batch id correct');
  is ($fc->position, 4, 'position correct');
  ok (!defined $fc->tag_index, 'tag_index not defined');
  is ($fc->entity_type, 'library_control', 'this is a control');

  $row = $rows[1];
  is ($row->position, 2, 'lane two present');
  $fc = $row->iseq_flowcell;
  is (ref $fc, 'WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell', 'retrieved flowcell row');
  is ($fc->id_flowcell_lims, 5992, 'batch id correct');
  is ($fc->position, 2, 'position correct');
  ok (!defined $fc->tag_index, 'tag_index not defined');
  is ($fc->entity_type, 'library', 'this is a library');

  $row = $rows[0];
  is ($row->position, 1, 'lane one present');
  ok(!defined $row->$LIMS_FK_COLUMN_NAME, 'lane 1 is duplicated in the flowcell table; foreign key for the flowcell table is absent');
  ok (!$row->iseq_flowcell, 'related object does not exist');

  $row = $rows[4];
  is ($row->position, 5, 'lane five present');
  ok(!defined $row->$LIMS_FK_COLUMN_NAME, 'lane 5 is not in the flowcell table; foreign key for the flowcell table is absent');

  $row = $rows[6];
  is ($row->position, 7, 'lane seven present');
  $fc = $row->iseq_flowcell;
  is (ref $fc, 'WTSI::DNAP::Warehouse::Schema::Result::IseqFlowcell', 'retrieved flowcell row');
  is ($fc->id_flowcell_lims, 5992, 'batch id correct');
  is ($fc->position, 7, 'position correct');
  is ($fc->tag_index, 1, 'tag_index is 1 - special case');
  is ($fc->entity_type, 'library', 'this is a library');
};

subtest 'loading mode data for insert size' => sub {
  plan tests => 7;

  my $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
         {id_run => 6998, position => 1, tag_index => [13,14,15]},
         {order_by => 'tag_index'}
  );
  is ($rs->count, 3, 'three records retrieved - test prerequisite');
  my $row = $rs->next;
  is($row->insert_size_num_modes, 2, 'num modes');
  is($row->insert_size_normal_fit_confidence, 0.34, 'confidence');
  $row = $rs->next;
  is($row->insert_size_num_modes, 1, 'num modes');
  is($row->insert_size_normal_fit_confidence, 0, 'negative confidence upped to zero');
  $row = $rs->next;
  is($row->insert_size_num_modes, 2, 'num modes');
  is($row->insert_size_normal_fit_confidence, 1, 'confidence capped to 1');
};

subtest 'not loading early stage runs' => sub {
  plan tests => 1;

  my %in = %{$init};
  $in{'id_run'} = 1246;
  $in{'verbose'} = 1;
  my $sid = $schema_npg->resultset('RunStatusDict')->search({description => 'run in progress'})->next->id_run_status_dict();
  $schema_npg->resultset('Run')->find(1246)->current_run_status->update( {id_run_status_dict => $sid,} );
  my $loader = npg_warehouse::loader::run->new(\%in);
  warnings_like { $loader->load() } [
    qr/Run status is \'run in progress\'/,
    qr/Too early to load run 1246, not loading/],
    'warning about not loading an early stage run';
};

subtest 'rna run' => sub {
  plan tests => 15;

  my $id_run = 24975;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, folder_name => '180130_MS6_24975_A_MS6073474-300V2',})}
    'folder glob reset lives - test prerequisite';

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  $in{'verbose'} = 0;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  lives_ok {$loader->load()} 'data is loaded into the product table';

  my $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => 24975,});
  is($rs->count(), 22, '22 rows in product table');

  my $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->find({id_run => $id_run, position => 1, tag_index=>1},);
  ok ($r, 'plex row for lane 1 tag index 1 is present');
  cmp_ok(sprintf('%.10f',$r->rna_exonic_rate), q(==), 0.68215317, 'loaded exonic rate matches source');
  cmp_ok(sprintf('%.10f',$r->rna_genes_detected), q(==), 12202, 'loaded genes detected matches source');
  cmp_ok(sprintf('%.10f',$r->rna_intronic_rate), q(==), 0.27704784, 'loaded intronic rate matches source');
  cmp_ok(sprintf('%.10f',$r->rna_norm_3_prime_coverage), q(==), 0.558965, 'loaded norm 3\' coverage matches source');
  cmp_ok(sprintf('%.10f',$r->rna_norm_5_prime_coverage), q(==), 0.38012463, 'loaded norm 5\' coverage matches source');
  cmp_ok(sprintf('%.10f',$r->rna_percent_end_2_reads_sense), q(==), 98.17338, 'loaded pct end 2 sense reads matches source');
  cmp_ok(sprintf('%.10f',$r->rna_rrna_rate), q(==), 0.020362793, 'loaded rrna rate matches source');
  cmp_ok(sprintf('%.10f',$r->rna_transcripts_detected), q(==), 71321, 'loaded transcripts detected matches source');
  cmp_ok(sprintf('%.10f',$r->rna_globin_percent_tpm), q(==), 2.71, 'loaded globin pct tpm matches source');
  cmp_ok(sprintf('%.10f',$r->rna_mitochondrial_percent_tpm), q(==), 6.56, 'loaded mitochondrial pct tpm matches source');
};

subtest 'gbs run' => sub {
  plan tests => 8;

  my $id_run = 25710;
  my $run_row = $schema_npg->resultset('Run')->find({id_run => $id_run, });
  lives_ok {$run_row->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  $run_row->unset_tag('fc_slotA');
  $run_row->unset_tag('fc_slotB');

  lives_ok {$schema_npg->resultset('Run')->update_or_create({
   folder_path_glob => $folder_glob, id_run => $id_run,
   folder_name => '180423_MS7_25710_A_MS6392545-300V2',})}
    'folder glob reset lives - test prerequisite';

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  $in{'verbose'} = 0;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  lives_ok {$loader->load()} 'data is loaded into the product table';

  my $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->find({id_run => $id_run, position => 1, tag_index=>60},);
  ok ($r, 'plex row for lane 1 tag index 60 is present');

  cmp_ok(sprintf('%.10f',$r->gbs_call_rate), q(==), 1, 'loaded gbs call rate matches source');
  cmp_ok(sprintf('%.10f',$r->gbs_pass_rate), q(==), 0.99, 'loaded gbs pass rate matches source');

  my @rows = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
    {id_run => $id_run})->all();
  for my $row (@rows) {
    is ($row->instrument_side, undef, 'instrument side is undefined');
    is ($row->workflow_type, undef, 'workflow type is undefined');
  }
};

subtest 'NovaSeq run with merged data' => sub {
  plan tests => 177;

  my $id_run = 26291;

  my $tdir = tempdir(CLEANUP => 1);
  dircopy('t/data/runfolders/with_merges', "$tdir/with_merges");

  # Create tracking record for a NovaSeq run with two lanes
  t::util::create_nv_run($schema_npg, $id_run, $tdir, 'with_merges');
  symlink 'Data/Intensities/BAM_basecalls_20180805-013153/no_cal',
          "$tdir/with_merges/Latest_Summary";

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  $in{'verbose'} = 0;
  my $loader  = npg_warehouse::loader::run->new(\%in);
  warnings_like { $loader->load() }
    [qr/Failed to find the component product row/],
    'warning when the component product row is not found';
  is($schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
    {id_run => $id_run})->count, 0, 'no rows loaded');
  
  my $archive_dir = 'Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive';
  my $lane_dir = "$tdir/with_merges/${archive_dir}/lane2";
  mkdir $lane_dir;
  mkdir "$lane_dir/qc";
  fcopy join(q[/],'t/data/runfolders/with_merges', $archive_dir, '26291_2.tag_metrics.json'),
    "$lane_dir/qc";

  $loader  = npg_warehouse::loader::run->new(\%in);
  lives_ok {$loader->load()} 'data loaded OK';
  my @rows = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
    {id_run => $id_run})->all();
  is ( scalar @rows, 2, 'two run-lane records created');
  for my $row (@rows) {
    is ($row->qc_seq, undef, 'seq qc undefined');
    is ($row->instrument_side, 'A', 'instrument side is A');
    is ($row->workflow_type, 'NovaSeqXp', 'workflow type is NovaSeqXp');
  }
  @rows = grep { $_->position == 1 } @rows;
  my $row1 = $rows[0];
  ok ($row1, 'row for lane 1 exists');
  my $expected = {
                   "interop_cluster_count_mean" =>  4091904,
                   "interop_cluster_count_pf_mean" =>  3121136.52840909,
                   "interop_cluster_count_pf_stdev" =>  35124.2526587081,
                   "interop_cluster_count_pf_total" =>  2197280116,
                   "interop_cluster_count_stdev" =>  0,
                   "interop_cluster_count_total" =>  2880700416,
                   "interop_cluster_density_mean" =>  2961263.95700836,
                   "interop_cluster_density_pf_mean" =>  2258730.68050473,
                   "interop_cluster_density_pf_stdev" =>  25419.018485059,
                   "interop_cluster_density_stdev" =>  4.65992365406662e-09,
                   "interop_cluster_pf_mean" =>  76.2758981737863,
                   "interop_cluster_pf_stdev" =>  0.85838408375925,
                 };

  for my $name (keys %{$expected}) {
    is ($row1->$name, $expected->{$name}, "value for $name is correct");
  } 

  @rows = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
    {id_run => $id_run})->all();
  is ( scalar @rows, 29, '29 product records created');
  is ($schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
    {id_run => $id_run, tag_index => undef})->count, 0,
    'no product record for a lane');

  for my $row (@rows) {
    is ($row->qc_seq, undef, 'seq qc undefined');
    is ($row->qc_lib, undef, 'lib qc undefined');
    is ($row->qc, undef, 'ovarall qc undefined');
  }

  my %plex_rows = map {$_->tag_index =>  $_}
                  grep { defined $_->position && ($_->position == 1) }
                  @rows;
  is ( scalar keys %plex_rows, 14, '14 records for plexes');
  for my $i ((0 .. 12, 888)) {
    my $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => $i)]);
    my $d = $c->digest;
    ok (exists $plex_rows{$i}, "record for plex $i created");
    is ($plex_rows{$i}->id_iseq_product, $d, "product id for plex $i");
    is ($plex_rows{$i}->iseq_composition_tmp, $c->freeze,
      "correct product composition JSON for plex $i");
  }

  my %merged_plex_rows = map {$_->tag_index =>  $_} grep { !defined $_->position } @rows;
  is ( scalar keys %merged_plex_rows, 1, '1 record for merged plexes');
  my $c = $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 1),
       $compon_pkg->new(id_run => $id_run, position => 2, tag_index => 1)]);
  my $d4merged = $c->digest;
  ok (exists $merged_plex_rows{1}, "record for merged plex 1 created");
  is ($merged_plex_rows{1}->id_iseq_product, $d4merged,
    'product id for merged plex 1');
  is ($merged_plex_rows{1}->id_run, $id_run, 'run id for merged plex 1');
  is ($merged_plex_rows{1}->tag_index, 1, 'tag index for merged plex 1');
  ok (!defined $merged_plex_rows{1}->position,
    'position for merged plex 1 is undefined');
  is ($merged_plex_rows{1}->iseq_composition_tmp, $c->freeze,
    'correct  product composition JSON for merged plex 1');

  # Create qc outcomes

  my $srs = $schema_qc->resultset('MqcOutcomeEnt');
  for my $p ((1 .. 2)) {
    my $q = {id_run => $id_run, position => $p};
    $q->{'id_seq_composition'} =
      t::util::find_or_save_composition($schema_qc, $q);
    $q->{'id_mqc_outcome'} = 3; #'Accepted final' 
    $srs->create($q);
  }

  my @queries =
    map { {id_run => $id_run, position => $_, tag_index => 1} }
    (1 .. 2);
  my $q = {};
  $q->{'id_seq_composition'} =
    t::util::find_or_save_composition($schema_qc, @queries);
  $q->{'id_mqc_outcome'} = 3; #'Accepted final'
  $schema_qc->resultset('MqcLibraryOutcomeEnt')->create($q);
  my $id = $q->{'id_seq_composition'};
  $q = {};
  $q->{'id_uqc_outcome'} = 2; # rejected
  $q->{'rationale'} = 'RT#456789';
  $q->{'username'} = 'cat';
  $q->{'modified_by'} = 'cat';
  $q->{'id_seq_composition'} = $id;
  my $uqc = $schema_qc->resultset('UqcOutcomeEnt')->create($q);

  # Load data again
  $loader = npg_warehouse::loader::run->new(\%in);
  lives_ok {$loader->load()} 'data is loaded';

  @rows = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
    {id_run => $id_run})->all();
  is ( scalar @rows, 2, 'two run-lane records');
  for my $row (@rows) {
    is ($row->qc_seq, 1, 'seq qc is a pass for lane ' . $row->position);
  }

  my @all = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
    {id_run => $id_run})->all();
  is (scalar @all, 29, '29 product records');
  my @qc_ed = grep {$_->qc_seq == 1} @all;
  is(scalar @qc_ed, 29, '29 rows have seq qc value set to 1');
  @qc_ed = grep {defined $_->qc && !defined $_->qc_lib} @all;
  is(scalar @qc_ed, 26,
    '26 rows have overall value pass and lib qc values undefined');
  is(scalar(grep {defined $_->qc_user} @qc_ed), 0, 'none have qc_user valuer set');
  @qc_ed =
    grep {(defined $_->qc && $_->qc == 1) &&
          (defined $_->qc_seq && $_->qc_seq == 1) &&
          (defined $_->qc_lib && $_->qc_lib == 1)}
    @all;
  is(scalar @qc_ed, 2, '2 rows have all qc values set to 1');
  my $row = $qc_ed[0];

  my @qcfrom_uqc =
    grep {(defined $_->qc && $_->qc == 0) &&
          (defined $_->qc_seq && $_->qc_seq == 1) &&
          (defined $_->qc_lib && $_->qc_lib == 1)}
    @all;
  is(scalar @qcfrom_uqc, 1, 'one row have qc value set from uqc');
  $row = $qcfrom_uqc[0];
  is ($row->id_iseq_product, $d4merged,
    'product with all qc values set is the merged plex 1');
  is ($row->qc_seq, 1, 'seq qc is a pass');
  is ($row->qc_lib, 1, 'lib qc is a pass');
  is ($row->qc, 0, 'overall qc is a fail');
  is ($row->qc_user, 0, 'user qc is a fail');
  $uqc->delete();
};

subtest 'run with merged data - linking to flowcell table' => sub {
  plan tests => 55;

  my $id_run = 26291;

  my $prs = $schema_wh->resultset($PRODUCT_TABLE_NAME);
  my $rs = $prs->search({id_run => $id_run});
  is ($rs->count, 29, "run $id_run number of rows in product table");
  is ($rs->search({$LIMS_FK_COLUMN_NAME => undef})->count, 29,
    'none of these rows are linked to the flowcell table');

  $schema_npg->resultset('Run')->find($id_run)->update({batch_id => 14178});
  $rs = $schema_wh->resultset('IseqFlowcell')
        ->search({id_flowcell_lims => 14178});
  while (my $row = $rs->next) {
    if ($row->position == 1) {
      my $new_ti = $row->tag_index == 168 ? 888 : ($row->tag_index - 12);
      $row->update({tag_index => $new_ti});
    }
  }

  my %in = %{$init};
  $in{'id_run'} = $id_run;
  $in{'verbose'} = 0;

  my $loader  = npg_warehouse::loader::run->new(\%in);
  ok(!$loader->lims_fk_repair, 'lims_fk_repair flag is false');
  lives_ok {$loader->load()} 'data is loaded';
  $rs = $prs->search({id_run => $id_run});
  is ($rs->count, 29, "run $id_run number of rows in product table");
  is ($rs->search({$LIMS_FK_COLUMN_NAME => undef})->count, 29,
    'none of these rows are linked to the flowcell table');

  $in{'lims_fk_repair'} = 1;
  $loader  = npg_warehouse::loader::run->new(\%in);
  ok($loader->lims_fk_repair, 'lims_fk_repair flag is true');

  lives_ok {$loader->load()} 'data is loaded';

  my $test_after_loading = sub {
    my $trs = $prs->search({id_run => $id_run});
    is ($trs->count, 29, "run $id_run number of rows in product table");
    is ($trs->search({$LIMS_FK_COLUMN_NAME => undef})->count, 16,
      '16 rows are not linked to the flowcell table');
    for my $ti (1 .. 11, 888) {
      my $row = $trs->search(
        {id_run => $id_run, position => 1, tag_index => $ti})->next;
      ok (defined $row->$LIMS_FK_COLUMN_NAME, "plex $ti is linked");
    }
  };

  $test_after_loading->();

  $in{'lims_fk_repair'} = 0;
  $loader  = npg_warehouse::loader::run->new(\%in);
  ok(!$loader->lims_fk_repair, 'lims_fk_repair flag is false');
  lives_ok {$loader->load()} 'data is loaded again';
  $test_after_loading->();

  $schema_wh->resultset($PRODUCTC_TABLE_NAME)->search({})->delete();
  $rs->delete();

  $rs = $prs->search({id_run => $id_run});
  is ($rs->count, 0, "all rows for run $id_run deleted from the product table");
  $in{'lims_fk_repair'} = 0;
  $loader  = npg_warehouse::loader::run->new(\%in);
  ok(!$loader->lims_fk_repair, 'lims_fk_repair flag is false');
  lives_ok {$loader->load()} 'data is loaded';
  $test_after_loading->();
};

subtest 'run with merged data - linking to product components' => sub {
  plan tests => 97;

  my $id_run = 26291;

  my $crs = $schema_wh->resultset($PRODUCTC_TABLE_NAME)->search({});
  is($crs->count, 30, '30 linking rows in total');
  my $mc_rs = $crs->search({num_components => 2});
  is ($mc_rs->count, 2, '2 rows for two-component products');
  my $sc_rs = $crs->search({num_components => 1});
  is ($sc_rs->count, 28, '28 rows for one-component products');
  is ($crs->search({component_index => 1})->count, 29, '29 linking rows have component index 1');
  is ($crs->search({component_index => 2})->count, 1, '1 linking rows has component index 2');

  for my $p ((1, 2)) {
    my $row = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
      {id_run => $id_run, position => $p, tag_index => 1})->next();
    is ($row->iseq_products->count(), 1, 'one link for this product');
    is ($row->iseq_product_components->count(), 2, 'two links for this product as a component');

    for my $ti ((0, 2 .. 12, 888)) {
      my $r = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
        {id_run => $id_run, position => $p,tag_index => $ti})->next();
      is ($r->iseq_products->count(), 1, 'one link for this product');
      is ($r->iseq_product_components->count(), 1, '1 link for this product as a component');
      is ($r->iseq_products->next->id_iseq_pr_components_tmp,
          $r->iseq_product_components->next->id_iseq_pr_components_tmp,
          'these two links are the same table row');
    }
  }

  my $row = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
    {id_run => $id_run, position => undef, tag_index => 1})->next();
  is ($row->iseq_product_components->count(), 0, 'no links for this product as a component');
  is ($row->iseq_products->count(), 2, 'two links for this product');
  my @prows = $row->iseq_products->search({}, {order_by => 'component_index'})->all();
  for my $i ((0,1)) {
    my $component = $prows[$i]->iseq_product_component;
    isa_ok ($component, 'WTSI::DNAP::Warehouse::Schema::Result::IseqProductMetric');
    is ($component->id_run, $id_run, 'id_run of the component');
    is ($component->position, $i+1, 'position of the component');
    is ($component->tag_index, 1, 'tag_index of the component');
  }
};

1;
