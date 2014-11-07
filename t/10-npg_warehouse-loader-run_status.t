use strict;
use warnings;
use English qw(-no_match_vars);
use Test::More tests => 18;
use Test::Exception;
use Moose::Meta::Class;
use npg_testing::db;

use_ok('npg_warehouse::loader::run_status');

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_npg;
lives_ok{ $schema_npg  = $util->create_test_db(
  q[npg_tracking::Schema], q[t/data/fixtures/npg])
} 'npg test db created';

{
  my $rsloader;
  foreach my $package (qw(WTSI::DNAP::Warehouse::Schema npg_warehouse::Schema)) {
    eval "require $package";
    my $e = $EVAL_ERROR;
    SKIP: {
      skip qq[Package $package not available], 8, if $e;
      
      my $wh;
      lives_ok{ $wh  = $util->create_test_db($package) } qq[test db created for $package];
      lives_ok {$rsloader  = npg_warehouse::loader::run_status->new( 
                                             schema_npg => $schema_npg, 
                                             schema_wh  => $wh
                                           )
      } 'object instantiated by passing schema objects to the constructor';
      isa_ok ($rsloader, 'npg_warehouse::loader::run_status');

      lives_ok {$rsloader->_copy_table('RunStatusDict')} 'copy run_status_dict table';
      lives_ok {$rsloader->_copy_table('RunStatus')} 'copy run_status table';
      lives_ok {$rsloader->copy_npg_tables()} 'copy  tables';

      is ($wh->resultset($rsloader->_prefix .'RunStatusDict')->search({})->count, 24, '24 rows loaded to the dictionary');
      is ($wh->resultset($rsloader->_prefix .'RunStatus')->search({})->count, 223, '223 rows loaded to the run status table');  
    }
  }
}

1;
