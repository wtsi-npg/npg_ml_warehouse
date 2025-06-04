use strict;
use warnings;
use Test::More tests => 13;
use Test::Exception;
use Moose::Meta::Class;

use npg_testing::db;

use_ok('npg_warehouse::loader::illumina::run_info');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_npg;
lives_ok{ $schema_npg  = $util->create_test_db(
  q[npg_tracking::Schema], q[t/data/fixtures/npg])
} 'npg test db created';
my $wh;
lives_ok{ $wh  = $util->create_test_db(
  q[WTSI::DNAP::Warehouse::Schema]) } qq[test wh db created];

{
  my $rsloader;
  lives_ok {$rsloader  = npg_warehouse::loader::illumina::run_info->new(
                                             schema_npg => $schema_npg, 
                                             schema_wh  => $wh
                                           )
  } 'object instantiated by passing schema objects to the constructor';
  isa_ok ($rsloader, 'npg_warehouse::loader::illumina::run_info');

  lives_ok {$rsloader->_copy_table('RunStatusDict')} 'copy run_status_dict table';
  lives_ok {$rsloader->_copy_table('RunStatus')} 'copy run_status table';
  lives_ok {$rsloader->_copy_table('Run')} 'copy run table';
  lives_ok {$rsloader->copy_npg_tables()} 'copy  tables';
  is ($wh->resultset('IseqRunStatusDict')->search({})->count, 24,
    '24 rows loaded to the dictionary');
  is ($wh->resultset('IseqRunStatus')->search({})->count, 261,
    'all rows loaded to the run status table');
  
  my @expected_runs = map { int }
                      qw/1246 1272 24975 25710 27116 3323 3351 3500 3519
                         3529 3622 3965 4025 4138 4333 4486 4799 6624 6642 6998/;
  my @rows = $wh->resultset('IseqRun')->search({})->all();
  is (@rows, @expected_runs, '20 rows loaded to iseq_run table');
  is_deeply ([sort map { $_->id_run } @rows], \@expected_runs,
    'data for Illumina runs only loaded to iseq_run table');
}

1;
