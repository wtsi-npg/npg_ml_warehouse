use strict;
use warnings;
use Test::More tests => 24;
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

1;