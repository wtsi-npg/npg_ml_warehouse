package t::util;

use strict;
use warnings;
use npg_tracking::glossary::composition::factory;
use npg_tracking::glossary::composition::component::illumina;

sub find_or_save_composition {
  my ($schema, $h) = @_;

  my $component_rs   = $schema->resultset('SeqComponent');
  my $composition_rs = $schema->resultset('SeqComposition');
  my $com_com_rs     = $schema->resultset('SeqComponentComposition');

  my @temp = %{$h};
  my %temp_hash = @temp;
  my $component_h = \%temp_hash;

  my $component =
    npg_tracking::glossary::composition::component::illumina->new($component_h);
  my $f = npg_tracking::glossary::composition::factory->new();
  $f->add_component($component);
  my $composition = $f->create_composition();
  my $composition_digest = $composition->digest;
  my $composition_row = $composition_rs->find({digest => $composition_digest});
  if (!$composition_row) {
    $component_h->{'digest'} = $component->digest;
    my $component_row = $component_rs->create($component_h);
    $composition_row = $composition_rs->create(
      {size => 1, digest => $composition_digest});
    $com_com_rs->create({size               => 1,
                         id_seq_component   => $component_row->id_seq_component,
                         id_seq_composition => $composition_row->id_seq_composition
                       });
  }
  return $composition_row->id_seq_composition;
}

1;
