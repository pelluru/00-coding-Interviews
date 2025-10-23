from patterns import *
def test_smoke():
    assert min_window('ADOBECODEBANC','ABC')=='BANC'
    assert two_sum_sorted([2,7,11,15],9)==[1,2]
    assert length_of_longest_substring('abcabcbb')==3
    assert search_range([5,7,7,8,8,10],8)==[3,4]
    assert num_islands([list('110'),list('010'),list('011')])==1
