#!/usr/bin/perl
use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Test::Deep;

use npg_testing::db;
use npg_warehouse::loader::irods_location;

my $new_locations = 't/data/irods_locations/new.json';
my $updated_locations = 't/data/irods_locations/update.json';
my $irods_locations_table_name = q[SeqProductIrodsLocation];
my $new_products  = [
  {
    "id_product" => "f97e7af62fa5cf2ceab52027e3ddc1bb6c265d517927b9abce8620042ac22eb6",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod",
    "irods_root_collection" => "/seq/4486",
    "irods_data_relative_path" => "4486_5.bam",
    "irods_secondary_data_relative_path" => undef,
    "id_seq_product_irods_locations_tmp" => ignore(),
    "created" => ignore(),
    "last_changed" => ignore(),
  },
  {
    "id_product" => "63019d04694e7af22fa98f3174b9120895ef4774b5e9f55f2da972270eade66b",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod",
    "irods_root_collection" => "/seq/6998",
    "irods_data_relative_path" => "6998_1#19.bam",
    "irods_secondary_data_relative_path" => undef,
    "id_seq_product_irods_locations_tmp" => ignore(),
    "created" => ignore(),
    "last_changed" => ignore(),
  },
  {
    "id_product" => "0e02e40a44fb8e14100621e227391ac8e74d3af10e0de0300e646a58e85a3ef1",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod",
    "irods_root_collection" => "/seq/6998",
    "irods_data_relative_path" => "6998_4#168.bam",
    "irods_secondary_data_relative_path" => undef,
    "id_seq_product_irods_locations_tmp" => ignore(),
    "created" => ignore(),
    "last_changed" => ignore(),
  }
];
my $updated_product =
  {
    "id_product"=> "8671acb3a98b1dad5ec481b82e20e11e5a8587c537f99381093c93cd6fd3251f",
    "seq_platform_name" => "illumina",
    "pipeline_name" => "npg-prod-alt-process",
    "irods_root_collection" => "/seq/4486",
    "irods_data_relative_path" => "4486_1.bam"
  };

sub get_product_row {
  my ($irods_locations, $product) = @_;
  return $irods_locations->schema_wh->resultset($irods_locations_table_name)->
      find_or_new($product, {key => 'pi_root_product'});
}

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_wh;
lives_ok { $schema_wh  = $util->create_test_db(
  q[WTSI::DNAP::Warehouse::Schema], q[t/data/fixtures/wh_npg])
} 'warehouse test db created';

subtest 'object creation' => sub {
  plan tests => 8;
  my $irods_locations;
  lives_ok {
    $irods_locations = npg_warehouse::loader::irods_location->new(
      schema_wh => $schema_wh,
      json_file => $new_locations,
    )
  } 'object instantiated by passing schema object to the constructor';
  isa_ok($irods_locations, 'npg_warehouse::loader::irods_location');
  is($irods_locations->json_file, $new_locations, 'json file set correctly');
  is($irods_locations->dry_run, 0, 'not a dry run by default');
  is($irods_locations->update, 0, 'does not update present rows by default');
  foreach my $product (@{$new_products}) {
    my $row = get_product_row($irods_locations, $product);
    is($row->in_storage, 0, 'new product not present in initial db');
  }
};

subtest 'dry-run' => sub {
  plan tests => 5;
  my $irods_locations = npg_warehouse::loader::irods_location->new(
    schema_wh => $schema_wh,
    json_file => $new_locations,
    dry_run   => 1
  );
  cmp_deeply($irods_locations->products, bag(
    subhashof($new_products->[0]),
    subhashof($new_products->[1]),
    subhashof($new_products->[2])), 'products read in correctly');
  lives_ok { $irods_locations->load_products }
    'product loading function succeeds';
  foreach my $product (@{$new_products}) {
    my $row = get_product_row($irods_locations, $product);
    is($row->in_storage, 0, 'dry-run has not added the product to the table');
  }
};

subtest 'new products' => sub {
  plan tests => 11;
  my $irods_locations = npg_warehouse::loader::irods_location->new(
    schema_wh => $schema_wh,
    json_file => $new_locations
  );
  lives_ok { $irods_locations->load_products }
    'product loading function succeeds';
  foreach my $product (@{$new_products}) {
    my $row = get_product_row($irods_locations, $product);
    is($row->in_storage, 1, 'new product loaded');
    cmp_deeply({$row->get_columns}, $product, 'correct values for new product');
  }
  lives_ok { $irods_locations->delete_products }
    'product deletion function succeeds';
  foreach my $product (@{$new_products}) {
    my $row = get_product_row($irods_locations, $product);
    is($row->in_storage, 0, 'product removed from table');
  }
};

subtest 'update product' => sub {
  plan tests => 5;
  my $irods_locations = npg_warehouse::loader::irods_location->new(
    schema_wh => $schema_wh,
    json_file => $updated_locations,
    update    => 1,
  );
  my $initial_row = get_product_row($irods_locations, $updated_product);
  is($initial_row->in_storage, 1, 'row in table before update');
  is($initial_row->get_column('pipeline_name'), 'npg-prod', 'row has correct value before update');
  lives_ok { $irods_locations->load_products }
    'product loading function succeeds';
  my $updated_row = get_product_row($irods_locations, $updated_product);
  is($updated_row->in_storage, 1, 'row in table after update');
  is($updated_row->get_column('pipeline_name'), 'npg-prod-alt-process', 'row has new value after update');
};

1;

