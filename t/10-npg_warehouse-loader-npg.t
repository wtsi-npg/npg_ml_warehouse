#########
# Author:        Marina Gourtovaia
# Maintainer:    $Author: dj3 $
# Created:       4 November 2010
# Last Modified: $Date: 2011-08-24 17:07:21 +0100 (Wed, 24 Aug 2011) $
# Id:            $Id: 10-npg_warehouse-loader-npg.t 14039 2011-08-24 16:07:21Z dj3 $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/trunk/t/10-npg_warehouse-loader-npg.t $
#

use strict;
use warnings;
use Test::More tests => 14;
use Test::Exception;
use Test::Deep;

use t::npg_warehouse::util;

use_ok('npg_warehouse::loader::npg');

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
          #  4779   # 
################################################################

my $util = t::npg_warehouse::util->new();
my $schema_npg;
my $index = 2;

{
  my $fixtures_path = q[t/data/fixtures/npg];
  lives_ok{ $schema_npg  = $util->create_test_db(q[npg_tracking::Schema], $fixtures_path) } 'npg test db created';
}

{
  my $npg;
  lives_ok {
       $npg  = npg_warehouse::loader::npg->new( 
                                             schema_npg => $schema_npg, 
                                             id_run => 1272,
                                           )
  } 'object instantiated by passing schema objects to the constructor';
  isa_ok ($npg, 'npg_warehouse::loader::npg');
  is ($npg->id_run, 1272, 'id run set correctly');
  is ($npg->verbose, 0, 'verbose mode is off by default');
}

{
  is(npg_warehouse::loader::npg->new(schema_npg => $schema_npg, id_run => 1272)->run_is_cancelled(),
                                    0, 'run 1272 is not cancelled');
  is(npg_warehouse::loader::npg->new(schema_npg => $schema_npg, id_run => 3519)->run_is_cancelled(),
                                    1, 'run 3519 is cancelled');
}

{
  is(npg_warehouse::loader::npg->new(schema_npg => $schema_npg, id_run => 3500)->run_is_paired_read(),
                                    1, 'run 3500 is paired read');
  is(npg_warehouse::loader::npg->new(schema_npg => $schema_npg, id_run => 3622)->run_is_paired_read(),
                                    0, 'run 3622 is paired read');
}

{
  cmp_deeply(npg_warehouse::loader::npg->new(schema_npg => $schema_npg, id_run => 1272)
        ->instrument_info, {name => q[IL20], model => q[1G],}, 'instr info for run 1272');
  cmp_deeply(npg_warehouse::loader::npg->new(schema_npg => $schema_npg, id_run => 3519)
        ->instrument_info, {name => q[IL42], model => q[HK],}, 'instr info for run 3519');
}

{
  my $npg;
  lives_ok {$npg  = npg_warehouse::loader::npg->new( schema_npg => $schema_npg )} 'object instantiated without id_run lives';
  is(join(q[ ], sort @{$npg->dev_cost_codes}), 'S0696 S0700 S0755', 'r&d cost codes');
}

1;