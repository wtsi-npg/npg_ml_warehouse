use strict;
use warnings;
use Test::More tests => 149;
use Test::Exception;
use Test::Warn;
use Test::Deep;
use Moose::Meta::Class;
use npg_testing::db;

use npg_qc::autoqc::qc_store;

my $RUN_LANE_TABLE_NAME      = q[IseqRunLaneMetric];
my $PRODUCT_TABLE_NAME       = q[IseqProductMetric];
my $LIMS_FK_COLUMN_NAME      = q[id_iseq_flowcell_tmp];
my @basic_run_lane_columns = qw/cycles
                                pf_cluster_count
                                pf_bases
                                paired_read
                                cancelled
                                instrument_name
                                instrument_model
                                raw_cluster_count
                                raw_cluster_density
                                pf_cluster_density
                                q30_yield_kb_forward_read
                                q30_yield_kb_reverse_read
                                q40_yield_kb_forward_read
                                q40_yield_kb_reverse_read/;

 my @columns =               qw/
     insert_size_quartile1 
     insert_size_quartile3 
     insert_size_median
     gc_percent_forward_read 
     gc_percent_reverse_read
     sequence_mismatch_percent_forward_read 
     sequence_mismatch_percent_reverse_read
     adapters_percent_forward_read 
     adapters_percent_reverse_read
     tags_decode_percent
     tags_decode_cv
                                   /;


use_ok('npg_warehouse::loader::run');
throws_ok {npg_warehouse::loader::run->new()}
    qr/Attribute \(id_run\) is required/,
    'error in constructor when id_run attr is not defined';

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my ($schema_npg, $schema_qc, $schema_wh);

lives_ok{ $schema_wh  = $util->create_test_db(q[WTSI::DNAP::Warehouse::Schema]) }
  'npgqc test db created';
lives_ok{ $schema_npg  = $util->create_test_db(q[npg_tracking::Schema],
  q[t/data/fixtures/npg]) } 'npg test db created';
lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]) } 'npgqc test db created';

my $autoqc_store =  npg_qc::autoqc::qc_store->new(use_db => 0, verbose => 0);

my $folder_glob = q[t/data/runfolders/];
my $user_id = 7;

my $plex_key = q[plexes];

my $init = { _autoqc_store => $autoqc_store,
             _schema_npg   => $schema_npg, 
             _schema_qc    => $schema_qc, 
             _schema_wh    => $schema_wh,
             verbose       => 0,
             reload_product_data => 0,
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

{
  my %in = %{$init};
  $in{'id_run'} = 1246;
  my $loader;

  lives_ok {$loader  = npg_warehouse::loader::run->new(\%in)}
    'loader object instantiated by passing schema objects to the constructor';
  isa_ok ($loader, 'npg_warehouse::loader::run');
  ok (!$loader->_old_forward_id_run, 'old forward id run is not set');
  $loader->load();
  my $rs = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 1246,});
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
  my @expected = (37,25284,617430,1,0,'IL20','1G',38831,undef,undef,3,0,4,0);
  foreach my $column (@basic_run_lane_columns) {
    push @found, $r->$column;
  }
  is_deeply(\@found, \@expected, 'run-lane data loaded correctly');
  
  ok (!$r->$LIMS_FK_COLUMN_NAME, 'lims fk not set');

  $r = $rs->next;
  is ($r->position, 2, 'result set for position 2');
  is ($r->run_pending->datetime, '2008-08-19T09:55:12', 'run pending for position 2');
  is ($r->qc_complete->datetime, '2008-09-25T13:18:20', 'run complete for position 2');

  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => 1246,});
  is ($rs->count, 1, '1 product row for run 1246');
  $r = $rs->next;
  is ($r->q30_yield_kb_forward_read, 3, 'forward read q30 for the product');
  is ($r->q40_yield_kb_forward_read, 4, 'forward read q40 for the product');
  ok (!$r->$LIMS_FK_COLUMN_NAME, 'lims fk not set');
}

{
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
  is ($r->raw_cluster_count, 185608, 'clusters_raw as expected');
  is ($r->pf_bases, 1430265+1430265 ,
    'pf_bases is summed up for two ends for a paired single folder run');
  is ($r->paired_read, 1, 'paired read flag updated correctly');
  is ($r->tags_decode_percent, undef, 'tags_decode_percent NULL where not loaded');
  is ($r->instrument_name, q[IL36] , 'instr name');
  is ($r->instrument_model, q[HK] , 'instr model');
  $r = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 3965,position=>2},)->next;
  is ($r->instrument_name, q[IL36] , 'instr name');
  is ($r->instrument_model, q[HK] , 'instr model');

  $in{'id_run'} = 3323;
  $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();
  $r = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 3323,position=>1},)->next;
  is($r->raw_cluster_density, undef, 'raw_cluster_density undefined');
  is($r->pf_cluster_density, undef, 'pf_cluster_density undefined'); 
}

{
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
          1 => {'raw_cluster_density' => 95465.880,  'pf_cluster_density' => 11496.220, 'q30_yield_kb_reverse_read' => '105906', 'q30_yield_kb_forward_read' => '98073', 'q40_yield_kb_forward_read' => '0'},
          2 => {'raw_cluster_density' => 325143.800, 'pf_cluster_density' => 82325.490, 'q30_yield_kb_reverse_read' => '1003112','q30_yield_kb_forward_read' => '563558'},
          3 => {'raw_cluster_density' => 335626.700, 'pf_cluster_density' => 171361.900,'q30_yield_kb_reverse_read' => '1011728','q30_yield_kb_forward_read' => '981688'},
          4 => {'raw_cluster_density' => 175608.400, 'pf_cluster_density' => 161077.600,'q30_yield_kb_reverse_read' => '714510', 'q30_yield_kb_forward_read' => '745267', 'q40_yield_kb_forward_read' => '56', 'q40_yield_kb_reverse_read' => '37',},
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
        qw/raw_cluster_density pf_cluster_density q30_yield_kb_forward_read q30_yield_kb_reverse_read/) {
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

  $expected->{4333}->{4}->{insert_size_quartile1} = 172;
  $expected->{4333}->{4}->{insert_size_quartile3} = 207;
  $expected->{4333}->{4}->{insert_size_median}    = 189;
  $expected->{4333}->{4}->{gc_percent_forward_read} = 44.89;
  $expected->{4333}->{4}->{gc_percent_reverse_read} = 44.88;
  $expected->{4333}->{4}->{sequence_mismatch_percent_forward_read} = 0.31;
  $expected->{4333}->{4}->{sequence_mismatch_percent_reverse_read} = 0.50;
  $expected->{4333}->{4}->{adapters_percent_forward_read} = 0.03;
  $expected->{4333}->{4}->{adapters_percent_reverse_read} = 0.02;
  $expected->{4333}->{4}->{tags_decode_percent} = undef;
  $expected->{4333}->{4}->{tags_decode_cv} = undef;

  $expected->{4333}->{1}->{gc_percent_forward_read} = 45.29;
  $expected->{4333}->{1}->{gc_percent_reverse_read} = 45.18;
  $expected->{4333}->{1}->{adapters_percent_forward_read} = 31.99;
  $expected->{4333}->{1}->{adapters_percent_reverse_read} = 25.93;
  $expected->{4333}->{1}->{tags_decode_percent} = 99.29;
  $expected->{4333}->{1}->{tags_decode_cv} = 55.1;

  $expected->{4333}->{8}->{insert_size_quartile3} = 207;
  $expected->{4333}->{8}->{insert_size_median}    = 189;
  $expected->{4333}->{8}->{tags_decode_percent} =81.94;
  $expected->{4333}->{8}->{tags_decode_cv} =122.4;
  #the third quartile has been skipped - the value is too large
 
  my $autoqc = {};
  while (my $row = $rs->next) {
    foreach my $column (@columns) { 
      $autoqc->{4333}->{$row->position}->{$column} = $row->$column;
    }
  }
  cmp_deeply($autoqc, $expected, 'loaded autoqc results');
}

{
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

  foreach my $pos (qw(1 2 4)) {
    ok (!$loader->_lane_is_indexed($pos), qq[lane $pos is not indexed]);
  }

  foreach my $pos (qw(3 5 6 7 8)) {
    ok ($loader->_lane_is_indexed($pos), qq[lane $pos is indexed]);
  }

  my @rows = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
       {id_run => $id_run, position => [5-8]},
  )->all();

  my $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => $id_run, tag_index => [1,2,3,4]},
  );
  is ($rs->count, 20, '20 rows in the plex table for run 4799 for plexes[1-4]');

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
  
  my @acolumns = @columns;
  pop @acolumns;
  pop @acolumns;

  foreach my $ycolumn (qw/ q20_yield_kb_forward_read q20_yield_kb_reverse_read
                           ref_match1_name ref_match1_percent
                           ref_match2_name ref_match2_percent /) {
    push @acolumns, $ycolumn;
  }
  
  my $found = {}; 
  while (my $row = $rs->next) {
    foreach my $column (@acolumns) {
      $found->{$row->position}->{$column} = $row->$column;
    }
  }
  my $e = {};
  $e->{1}->{insert_size_quartile1} = undef;
  $e->{1}->{insert_size_quartile3} = undef;
  $e->{1}->{insert_size_median} = undef;
  $e->{1}->{gc_percent_forward_read} = 45.29;
  $e->{1}->{gc_percent_reverse_read} = 45.18;
  $e->{1}->{sequence_mismatch_percent_forward_read} = undef;
  $e->{1}->{sequence_mismatch_percent_reverse_read} = undef;
  $e->{1}->{adapters_percent_forward_read} = 31.99;
  $e->{1}->{adapters_percent_reverse_read} = 25.93;
  $e->{1}->{q20_yield_kb_forward_read} = 46671;
  $e->{1}->{q20_yield_kb_reverse_read} = 39877;
  $e->{4}->{insert_size_quartile1} = 172;
  $e->{4}->{insert_size_quartile3} = 207;
  $e->{4}->{insert_size_median} = 189;
  $e->{4}->{gc_percent_forward_read} = 44.89;
  $e->{4}->{gc_percent_reverse_read} = 44.88;
  $e->{4}->{sequence_mismatch_percent_forward_read} = 0.31;
  $e->{4}->{sequence_mismatch_percent_reverse_read} = 0.5;
  $e->{4}->{adapters_percent_forward_read} = 0.03;
  $e->{4}->{adapters_percent_reverse_read} = 0.02;
  $e->{4}->{q20_yield_kb_forward_read} = 1455655;
  $e->{4}->{q20_yield_kb_reverse_read} = 1393269;
  $e->{1}->{ref_match1_name} = q[Homo sapiens 1000Genomes];
  $e->{1}->{ref_match1_percent} = 95.7;
  $e->{1}->{ref_match2_name} = q[Gorilla gorilla gorilla];
  $e->{1}->{ref_match2_percent} = 85.2;
  $e->{4}->{ref_match1_name} = q[Homo sapiens 1000Genomes];
  $e->{4}->{ref_match1_percent} = 97.2;
  $e->{4}->{ref_match2_name} = q[Gorilla gorilla gorilla];
  $e->{4}->{ref_match2_percent} = 87.2;
 
  is_deeply ($found, $e, 'lane product autoqc results');  
}

{
  my $id_run = 6624;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '110731_HS17_06624_A_B00T5ACXX', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

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

  ok ($loader->_lane_is_indexed(2), 'lane 2 is indexed');
  is ($schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>2,tag_index=>undef})->count,
    0, 'lane 2 is not in product table');
  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->find({id_run=>$id_run,position=>2,tag_index=>168});
  is($plex->q30_yield_kb_reverse_read, 304, 'q30 plex reverse');
  is($plex->q40_yield_kb_forward_read, 210, 'q40 plex forward');
  is($plex->tag_sequence4deplexing(), 'ACAACGCA', 'lane 2 tag index 168 tag sequence');
  is($plex->tag_decode_count(), 1277701, 'lane 2 tag index 168 count');
  cmp_ok(sprintf('%.2f', $plex->tag_decode_percent()), q(==), 0.73, ,
    'lane 2 tag index 168 percent');

  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>3,tag_index=>1})->first;
  cmp_ok(sprintf('%.2f',$plex->mean_bait_coverage()), q(==), 41.49, 'mean bait coverage');
  cmp_ok(sprintf('%.2f',$plex->on_bait_percent()), q(==), 68.06, 'on bait percent');
  cmp_ok(sprintf('%.2f',$plex->on_or_near_bait_percent()), q(==), 88.92, 'on or near bait percent');

  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>3,tag_index=>4})->first;
  cmp_ok(sprintf('%.2f',$plex->num_reads()), q(==), 33605036, 'bam number of reads');
  cmp_ok(sprintf('%.2f',$plex->percent_mapped()), q(==), 96.12, 'bam (nonphix) mapped percent');
  cmp_ok(sprintf('%.2f',$plex->percent_duplicate()), q(==), 1.04, 'bam (nonphix) duplicate percent');

  ok ($loader->_lane_is_indexed(4), 'lane 4 is indexed');
  is ($schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>4,tag_index=>undef})->count,
    0, 'lane 4 is not in product table');
  $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->find({id_run=>$id_run,position=>4,tag_index=>0});
  is($plex->q30_yield_kb_reverse_read, 99353, 'q30 plex reverse');
  is($plex->q40_yield_kb_forward_read, 72788, 'q40 plex forward');
}

{
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

  ok (!$loader->_lane_is_indexed(1), 'lane 1 is not indexed');
  my $lane = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>1,tag_index=>undef})->first;
  ok ($lane, 'product row for lane 1 is present');
  ok ($loader->_lane_is_indexed(2), 'lane 2 is indexed');
  $lane = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>2,tag_index=>undef})->first;
  ok (!$lane, 'product row for lane 2 is not present');

  ok (!$loader->_lane_is_indexed(3), 'lane 3 is not indexed');
  $lane = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run => $id_run, position=>3,tag_index=>undef},)->first;
  ok ($lane, 'product row for lane 3 is present');
  cmp_ok(sprintf('%.2f',$lane->num_reads()), q(==), 308368522, 'bam number of reads');
  cmp_ok(sprintf('%.2f',$lane->percent_mapped()), q(==), 98.19, 'bam mapped percent');
  cmp_ok(sprintf('%.2f',$lane->percent_duplicate()), q(==), 24.63, 'bam duplicate percent');

  my $plex = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>2,tag_index=>4})->first;
  ok ($plex, 'plex row for lane 2 tag index 4 is present');
  cmp_ok(sprintf('%.2f',$plex->human_percent_mapped()), q(==), 55.3, 'bam human mapped percent');
  cmp_ok(sprintf('%.2f',$plex->human_percent_duplicate()), q(==), 68.09, 'bam human duplicate percent');
  cmp_ok(sprintf('%.2f',$plex->num_reads()), q(==), 138756624, 'bam (nonhuman) number of reads');
  cmp_ok(sprintf('%.2f',$plex->percent_mapped()), q(==), 96.3, 'bam (nonhuman) mapped percent');
  cmp_ok(sprintf('%.2f',$plex->percent_duplicate()), q(==), 6.34, 'bam (nonhuman) duplicate percent');

  my $num_products1 = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run})->count;
  my $old_value = $loader->_data->{$PRODUCT_TABLE_NAME}->[1]->{tag_decode_percent};
  $loader->_data->{$PRODUCT_TABLE_NAME}->[1]->{tag_decode_percent} = $old_value + 1;
  my $tag_index = $loader->_data->{$PRODUCT_TABLE_NAME}->[1]->{tag_index};
  my $pos = $loader->_data->{$PRODUCT_TABLE_NAME}->[1]->{position};
  $loader->load();
  my $num_products2 = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run})->count;
  is ($num_products2, $num_products1 + 1, 'one row duplicated on reloading');
  is ($schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>$pos,tag_index=>$tag_index})->count(),
    2, 'edited row was duplicated'); 

  $in{'reload_product_data'} = 1;
  $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();
  $num_products2 = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run})->count;
  is ($num_products2, $num_products1, 'no duplication');
  $loader->_data->{$PRODUCT_TABLE_NAME}->[1]->{tag_decode_percent} = $old_value + 1;
  $loader->load();
  $num_products2 = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run})->count;
  is ($num_products2, $num_products1, 'no duplication');
  $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search({id_run=>$id_run,position=>$pos,tag_index=>$tag_index});
  is ($rs->count, 1, 'edited row was not duplicated');
  is ($rs->next->tag_decode_percent, $old_value + 1, 'value updated');
}

1;
