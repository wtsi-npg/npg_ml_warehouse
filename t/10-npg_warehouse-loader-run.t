use strict;
use warnings;
use Test::More tests => 70;
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
             verbose       => 1,
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
  $in{'verbose'} = 0;
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
  is($r->paired_read, 1, 'paired read flag updated correctly');

  $in{'id_run'} = 3323;
  $loader  = npg_warehouse::loader::run->new(\%in);
  $loader->load();
  $r = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search({id_run => 3323,position=>1},)->next;
  is($r->raw_cluster_density, undef, 'raw_cluster_density undefined');
  is($r->pf_cluster_density, undef, 'pf_cluster_density undefined'); 
}

{
  my %in = %{$init};
  $in{'id_run'} = 4333;
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
  is ($rs->count, 8, '8 product rows for run 4333');
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

  my @rows = $schema_wh->resultset($RUN_LANE_TABLE_NAME)->search(
       {id_run => 4799, position => [5-8]},
  )->all();

  #my %deplexing_stats = map {$_->position => $_->tags_decode_percent } @rows;
  #my $expected = {5 => 99.88, 6 => 99.48, 7 => 98.93, 8 => 97.08,};
  #is_deeply (\%deplexing_stats, $expected, 'tag decoding percent');
  #use Data::Dumper;
  #diag Dumper \%deplexing_stats;
  #%deplexing_stats = map {$_->position => $_->tags_decode_cv } @rows;
  #$expected = {5 => 173.20, 6 => 173.19, 7 => 173.06, 8 => 27.45,};
  #is_deeply (\%deplexing_stats, $expected, 'tag decoding cv');
  #diag Dumper \%deplexing_stats;

  my $rs = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => 4799, tag_index => [1,2,3,4]},
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
      $tag_info->{$index}->{tag_decode_percent} = $r->tag_decode_percent;
      $tag_info->{$index}->{tag_sequence} = $r->tag_sequence4deplexing;
    }
  }
  cmp_deeply($tag_info, $expected_tag_info, 'tag info for runs 4799 position 7 plexex 1-4');

  my $result = $schema_wh->resultset($PRODUCT_TABLE_NAME)->search(
       {id_run => 4799, position=>7, tag_index => 5},
  )->first;
  ok($result, 'a row for a tag index that is not listed in lims exists');
  is($result->$LIMS_FK_COLUMN_NAME, undef, 'lims foreign key not defined');
}

1;