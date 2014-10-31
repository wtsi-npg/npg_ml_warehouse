use strict;
use warnings;
use Test::More tests => 2;
use Test::Exception;

use_ok('npg_warehouse::loader::run');
throws_ok {npg_warehouse::loader::run->new()}
    qr/Attribute \(id_run\) is required/,
    'error in constructor when id_run attr is not defined';

1;