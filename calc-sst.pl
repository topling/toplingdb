#!/usr/bin/perl -w

my %suf = (
	KB => 1e3,
	MB => 1e6,
	GB => 1e9,
	TB => 1e12,
);
while (<>) {
	my @F=();
	my $levels;
	if (/Base level 0, inputs: \[([^\]]*)\]/) {
		my $lev0 = $1;
		$levels = $';
		while ($lev0 =~ /\((\d+)(.B)\)/g) {
			my $mul = $suf{$2};
			push @F, $1 * $suf{"$2"};
		}
	}
	else {
		$levels = $_;
	}
	while ($levels =~ /\[([^\]]*)\]/g) {
		my $lev = $1;
		my $size = 0;
		while ($lev =~ /\((\d+)(.B)\)/g) {
			my $mul = $suf{$2};
			$size += $1 * $suf{"$2"};
		}
		if ($size > 0) {
			push @F, $size;
		}
	}
	@F = sort {$a<=>$b} @F;
	my $sum = 0;
	for (my $i = 0; $i < scalar(@F); $i++) {
		$sum += $F[$i];
	}
	#print "max/sum = ", $F[$#F] / $smallsum, "\n";
	my $max = $F[$#F];
	if (scalar(@F) >= 2) {
		printf("%9.6f  %9.6f  %9.6f ;", $sum / $max, $max / ($sum - $max), $max/$sum);
		for my $val (@F) {
			if ($val < 1e9) {
				printf(" %8.3f'M", $val/1e6);
			}
			else {
				printf(" %8.3f'G", $val/1e9);
			}
		}
		printf("\n");
	}
}
