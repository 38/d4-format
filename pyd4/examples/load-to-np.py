import pyd4

hg002 = pyd4.D4File("/home/haohou/base2/data/hg002.d4");
print(hg002.load_to_np("1")[0].mean())
