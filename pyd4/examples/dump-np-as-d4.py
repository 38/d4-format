import pyd4

input = pyd4.D4File("hg002.d4")

writer = input \
    .create_on_same_genome("/tmp/test.d4", ["1", "2"]) \
    .for_bit_array() \
    .get_writer()

for seq in ["1", "2"]:
    chr_data = input.load_to_np(seq);
    writer.write_np_array(seq, 0, chr_data)
