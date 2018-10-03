package npg_warehouse::loader::autoqc;

use Carp;
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_qc::autoqc::qc_store;
use npg_qc::autoqc::qc_store::options qw/$LANES $PLEXES/;
use npg_qc::autoqc::qc_store::query;
use npg_qc::autoqc::results::collection;

our $VERSION = '0';

## no critic (ProhibitUnusedPrivateSubroutines)

Readonly::Scalar our $INSERT_SIZE_QUARTILE_MAX_VALUE => 65_535; #max for MYSQL smallint unsigned

Readonly::Hash   our %AUTOQC_MAPPING  => {
     gc_fraction =>      {
                           'gc_percent_forward_read' => 'forward_read_gc_percent',
                           'gc_percent_reverse_read' => 'reverse_read_gc_percent',
                         },
     sequence_error =>   {
                           'sequence_mismatch_percent_forward_read' => 'forward_average_percent_error',
                           'sequence_mismatch_percent_reverse_read' => 'reverse_average_percent_error',
                         },
     adapter     =>      {
                           'adapters_percent_forward_read' => 'forward_percent_contam_reads',
                           'adapters_percent_reverse_read' => 'reverse_percent_contam_reads',
                         },
     pulldown_metrics => { 'mean_bait_coverage'      => 'mean_bait_coverage',
                           'on_bait_percent'         => 'on_bait_bases_percent',
                           'on_or_near_bait_percent' => 'selected_bases_percent',
                         },
     verify_bam_id    => { 'verify_bam_id_score'         => 'freemix',
                           'verify_bam_id_average_depth' => 'avg_depth',
                           'verify_bam_id_snp_count'     => 'number_of_snps',
                         },
     rna_seqc         => {
                           'rna_exonic_rate'               => 'exonic_rate',
                           'rna_percent_end_2_reads_sense' => 'end_2_pct_sense',
                           'rna_rrna_rate'                 => 'rrna_rate',
                           'rna_genes_detected'            => 'genes_detected',
                           'rna_norm_3_prime_coverage'     => 'end_3_norm',
                           'rna_norm_5_prime_coverage'     => 'end_5_norm',
                           'rna_intronic_rate'             => 'intronic_rate',
                           'rna_transcripts_detected'      => 'transcripts_detected',
                           'rna_globin_percent_tpm'        => 'globin_pct_tpm',
                           'rna_mitochondrial_percent_tpm'  => 'mt_pct_tpm',
                         },
     genotype_call    => {
                           'gbs_call_rate'                 => 'genotype_call_rate',
                           'gbs_pass_rate'                 => 'genotype_passed_rate',
                         },
};

Readonly::Scalar my $HUNDRED  => 100;

=head1 NAME

npg_warehouse::loader::autoqc

=head1 SYNOPSIS
 
 my $id_run = 222;
 my $autoqc_hash = npg_:warehouse::loader::autoqc->new(plex_key=>q[plex])->retrieve($id_run);

=head1 DESCRIPTION

Retrieval of autoqc data for loading to the warehouse

=head1 SUBROUTINES/METHODS

=cut

=head2 verbose

Verbose flag

=cut
has 'verbose'      => ( isa        => 'Bool',
                        is         => 'ro',
                        required   => 0,
                        default    => 0,
                      );

=head2 autoqc_store

A driver to retrieve autoqc objects, required attribute.

=cut
has 'autoqc_store' =>    ( isa        => 'npg_qc::autoqc::qc_store',
                           is         => 'ro',
                           required   => 1,
                         );

=head2 plex_key

Name of the key to use in data structures for plex data.

=cut
has 'plex_key' =>   ( isa             => 'Str',
                      is              => 'ro',
                      required        => 1,
		    );

=head2 _tag_metrics_rpt_keys

Signatures of available tag metrics objects (as run:position:tag keys)

=cut
has '_tag_metrics_rpt_keys' => ( isa     => 'HashRef',
                                 is      => 'ro',
                                 default => sub { return {}; },
		               );

sub _truncate_float {
    my ($self, $value) = @_;
    ##no critic (ProhibitEscapedMetacharacters ProhibitEnumeratedClasses)
    if ($value && $value =~ /[0-9]*\.[0-9]+/smx) {
        $value = sprintf '%.2f', $value;
    }
    return $value;
}


sub _copy_fields {
    my ($self, $source, $target, $position, $tag_index) = @_;

    foreach my $column (keys %{$source}) {
        my $value = $source->{$column};
        if (!defined $tag_index) {
            $target->{$position}->{$column} = $value;
	} else {
            $target->{$position}->{$self->plex_key}->{$tag_index}->{$column} = $value;
	}
    }
    return;
}

sub _insert_size {
    my ($self, $result, $autoqc) = @_;
    my $data = {};

    $data->{'insert_size_quartile1'} = $result->quartile1;
    $data->{'insert_size_quartile3'} = $result->quartile3;
    $data->{'insert_size_median'}    = $result->median;

    foreach my $key (keys %{$data}) {
        if ($data->{$key} && $data->{$key} > $INSERT_SIZE_QUARTILE_MAX_VALUE) {
	    delete $data->{$key};
	    if($self->verbose) {
		carp sprintf 'SKIPPING %s for id_run %i position %i: value %i out of range',
                $key, $result->id_run, $result->position, $data->{$key};
	    }
        }
    }

    $data->{'insert_size_num_modes'}             = $result->norm_fit_nmode;
    my $v = $result->norm_fit_confidence;
    if (defined $v) {
        if ($v > 1) {
            $v = 1;
        } elsif ($v < 0) {
	    $v = 0;
	}
        $data->{'insert_size_normal_fit_confidence'} = $self->_truncate_float($v);
    }
    $self->_copy_fields($data, $autoqc, $result->position, $result->tag_index);
    return;
}

sub _qX_yield {
    my ($self, $result, $autoqc) = @_;

    my $data = {};
    foreach my $read (qw/1 2/) {
        foreach my $quality (qw/20 30 40/) {
            my $autoqc_method_name = sprintf 'yield%s_q%s', $read, $quality;
            my $wh_column_name     = sprintf 'q%s_yield_kb_%s_read',
                $quality, ($read eq '1') ? 'forward' : 'reverse';
            my $value = $result->$autoqc_method_name;
            if (defined $value) {
                $data->{$wh_column_name} = $result->$autoqc_method_name;
            }
        }
    }
    $self->_copy_fields($data, $autoqc, $result->position, $result->tag_index);
    return;
}

sub _ref_match {
    my ($self, $result, $autoqc) = @_;

    my $organisms = $result->ranked_organisms;
    my $percent_counts = $result->percent_count;
    my $prefix = q[ref_match];

    foreach my $count ((1,2)) {
	if (scalar @{$organisms} >= $count) {
            my $data = {};
            my $organism = $organisms->[$count-1];
            $data->{$prefix.$count.q[_percent]} = $percent_counts->{$organism};
            my $strain = $result->reference_version->{$organism};
            $organism =~  s/_/ /xms;
            $data->{$prefix.$count.q[_name]}  = join q[ ], $organism, $strain;
	    $self->_copy_fields($data, $autoqc, $result->position, $result->tag_index);
	}
    }
    return;
}

sub _contamination {
    my ($self, $result, $autoqc) = @_;

    my $organisms = $result->ranked_organisms;
    my $contamination = $result->normalised_contamination;
    my $prefix = q[contaminants_scan_hit];

    foreach my $count ((1,2)) {
	if (scalar @{$organisms} >= $count) {
            my $data = {};
            my $organism = $organisms->[$count-1];
            $data->{$prefix.$count.q[_name]}  = $organism;
            $data->{$prefix.$count.q[_score]} = $self->_truncate_float($contamination->{$organism});
	    $self->_copy_fields($data, $autoqc, $result->position, $result->tag_index);
	}
    }
    return;
}

sub _tag_metrics {
    my ($self, $result, $autoqc) = @_;

    my $position = $result->position;
    $self->_tag_metrics_rpt_keys->{$result->rpt_key} = 1;
    if (defined $result->matches_pf_percent) {
        $autoqc->{$position}->{tags_decode_percent} = $self->_truncate_float(
                       $result->perfect_matches_percent + $result->one_mismatch_percent);
    }

    if (defined $result->variance_coeff) {
	$autoqc->{$position}->{tags_decode_cv} =
	    $self->_truncate_float($result->variance_coeff(1));
    }

    if (defined $result->tag_hops_percent) {
        $autoqc->{$position}->{tag_hops_percent} = $result->tag_hops_percent;
    }

    if (defined $result->tag_hops_power) {
        $autoqc->{$position}->{tag_hops_power} = $result->tag_hops_power;
    }

    foreach my $i (keys %{$result->tags}) {
        if ($i != 0) { # no tag sequence for tag zero
            $autoqc->{$position}->{$self->plex_key}->{$i}->{'tag_sequence'} = $result->tags->{$i};
	}
        $autoqc->{$position}->{$self->plex_key}->{$i}->{'tag_decode_count'} = $result->reads_pf_count->{$i};
        $autoqc->{$position}->{$self->plex_key}->{$i}->{'tag_decode_percent'} =
                   $self->_truncate_float($result->matches_pf_percent->{$i} * $HUNDRED);
    }
    return;
}

sub _tag_decode_stats {
    my ($self, $result, $autoqc) = @_;

    # Do not load tag decode stats if tag metrics data available for the same entity
    if (exists $self->_tag_metrics_rpt_keys->{$result->rpt_key}) { return; }

    my $position = $result->position;
    my $value = $result->decoding_perc_good;
    if (defined $value) {
        $autoqc->{$position}->{tags_decode_percent} = $self->_truncate_float($value);
    }

    if (defined $result->variance_coeff) {
	$autoqc->{$position}->{tags_decode_cv} =
	    $self->_truncate_float($result->variance_coeff('all'));
    }

    my $tags = $result->tag_code;
    if ($tags && scalar keys %{$tags}) {
	my $good = $result->distribution_perc_good;
	my $good_count = $result->distribution_good;
	foreach my $tag_index (keys %{$tags}) {
	    $autoqc->{$position}->{$self->plex_key}->{$tag_index}->{'tag_sequence'} = $tags->{$tag_index};
	    if ($good && exists $good->{$tag_index}) {
		$autoqc->{$position}->{$self->plex_key}->{$tag_index}->{'tag_decode_percent'} =
                   $self->_truncate_float($good->{$tag_index});
		$autoqc->{$position}->{$self->plex_key}->{$tag_index}->{'tag_decode_count'} =
                   $good_count->{$tag_index};
	    }
	}
    }
    return;
}

sub _bam_flagstats {
    my ($self, $result, $autoqc) = @_;

    my $position = $result->position;
    my $tag_index = $result->tag_index;

    my $check_name = $result->check_name;
    if ($check_name =~ /phix/xsmg){ return; }

    $check_name =~ s/[ ]flagstats//xsmg;
    $check_name =~ s/[ ]/_/xsmg;
    foreach my $method (qw(percent_mapped_reads percent_duplicate_reads)) {
        if (my $r = $result->$method ) {
            my $m = $method;
            $m =~ s/_reads\z//xsmg;
            my $c = $check_name;
            $c =~ s/_nonhuman//xsmg;
            $c =~ s/_xahuman/_human/xsmg;
            $c =~ s/_yhuman/_human/xsmg;
            $self->_copy_fields({$c.q[_].$m => $r}, $autoqc, $position, $tag_index);
        }
    }

    if ($check_name =~ /_human/xsmg) { return; }
    my $num_reads = $result->total_reads;
    $self->_copy_fields({bam_num_reads => $num_reads,}, $autoqc, $position, $tag_index);
    my $chimeric_reads = $self->_truncate_float(
      ($num_reads && $result->mate_mapped_defferent_chr_5)
        ? ($result->mate_mapped_defferent_chr_5 * $HUNDRED / $num_reads)
        : 0.00);
    $self->_copy_fields({chimeric_reads_percent => $chimeric_reads}, $autoqc, $position, $tag_index);
    return;
}

sub _upstream_tags {
    my ($self, $result, $autoqc) = @_;
    my $position = $result->position;
    my $tag_index = $result->tag_index;
    my $total = $result->total_lane_reads;
    my $unexpected_tags_percent = $self->_truncate_float($total ? $result->tag0_perfect_match_reads * $HUNDRED / $total : 0.00);
    $self->_copy_fields({unexpected_tags_percent => $unexpected_tags_percent}, $autoqc, $position, $tag_index);
    return;
}

sub _split_stats {
    my ($self, $result, $autoqc) = @_;
    my $position = $result->position;
    my $check_name = $result->check_name;
    $check_name =~ s/[ ]stats//xsmg;
    $check_name =~ s/[ ]/_/xsmg;
    my $tot;
    if (defined $result->num_aligned_merge && defined $result->num_not_aligned_merge) {
      $tot = $result->num_aligned_merge + $result->num_not_aligned_merge;
    }
    my $pc = $tot ? ($HUNDRED * $result->num_aligned_merge) / $tot : undef;
    $self->_copy_fields({$check_name.q[_percent] => $pc}, $autoqc, $result->position, $result->tag_index);
    return;
}

sub _genotype {
    my ($self, $result, $autoqc) = @_;

    my $data = {};
    my $sample_name_match = $result->sample_name_match;
    if (defined $sample_name_match) {
        # Probably, the data can be fixed instead of setting to 0 if false
        $data->{genotype_sample_name_match} = join q[/], $sample_name_match->{match_count} || 0, $sample_name_match->{common_snp_count} || 0;
    }

    my $sample_name_relaxed_match = $result->sample_name_relaxed_match;
    if (defined $sample_name_relaxed_match) {
        $data->{genotype_sample_name_relaxed_match} = join q[/], $sample_name_relaxed_match->{match_count}, $sample_name_relaxed_match->{common_snp_count};
    }

    my $bam_gt_depths_string = $result->bam_gt_depths_string;
    if (defined $bam_gt_depths_string) {
        my $tot = 0;
        my $c = 0;
        for my $v (split /;/smx, $bam_gt_depths_string) {
		$tot += $v;
                $c++;
        }

        my $mean_depth = sprintf '%.02f', ($tot / $c);

        $data->{genotype_mean_depth} = $mean_depth;
    }

    $self->_copy_fields($data, $autoqc, $result->position, $result->tag_index);

    return;
}

sub _autoqc_check {
    my ($self, $result, $autoqc) = @_;

    my $component = $result->composition->get_component(0);
    my $position = $component->position;
    my $tag_index = $component->tag_index;

    my $map = $AUTOQC_MAPPING{$result->class_name};
    foreach my $key (keys %{$map}) {
        my $method = $map->{$key};
        my $value = $result->$method;
        if (defined $value) {
            if ( $key !~ /\Averify_bam_id|\Arna/xms ) {
                $value = $self->_truncate_float($value);
            }
            if (! defined $tag_index) {
                $autoqc->{$position}->{$key} = $value;
            } else {
                $autoqc->{$position}->{$self->plex_key}->{$tag_index}->{$key} = $value;
            }
        }
    }
    return;
}

=head2 retrieve

Retrieves autoqc results for a run. Skips results for multi-component entities.

=cut
sub retrieve {
    my ($self, $id_run, $npg_schema) = @_;

    my $query1 = npg_qc::autoqc::qc_store::query->new(
                                                id_run              => $id_run,
                                                option              => $LANES,
                                                npg_tracking_schema => $npg_schema
                                                     );
    my $query2 = npg_qc::autoqc::qc_store::query->new(
                                                id_run              => $id_run,
                                                option              => $PLEXES,
                                                npg_tracking_schema => $npg_schema
                                                     );
    my $collection = npg_qc::autoqc::results::collection->join_collections(
                     $self->autoqc_store->load($query1), $self->autoqc_store->load($query2));
    $collection->sort_collection(q[check_name]); # tag metrics object are after tag decode stats now

    my $i = $collection->size - 1;
    my $autoqc = {};
    while ($i >= 0) { # iterating from tail to head
        my $result = $collection->get($i);
        $i--;
        if ($result->composition->num_components() > 1) {
            next;
        }
        my $method_name = exists $AUTOQC_MAPPING{$result->class_name} ? q[_autoqc_check] : q[_] . $result->class_name;
        if ($self->can($method_name)) {
            $self->$method_name($result, $autoqc);
        }
    }
    return $autoqc;
}


__PACKAGE__->meta->make_immutable;
1;
__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Readonly

=item Moose

=item MooseX::StrictConstructor

=item npg_qc::autoqc::qc_store

=item npg_qc::autoqc::qc_store::options

=item npg_qc::autoqc::qc_store::query

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Limited

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
