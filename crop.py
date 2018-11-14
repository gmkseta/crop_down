from PIL import Image

def crop_img(input_path,output_path,size):
    im = Image.open(input_path)

    width = im.size[0]
    height = im.size[1]
    n_h = 0
    n_w = 0

    if width>height:
        #작은쪽으로 맞춰야지
        n_h = size
        ratio=width/height
        n_w = size*ratio
    else:
        n_w = size
        ratio=height/width
        n_h = size*ratio

    im2 = im.resize((int(n_w),int(n_h)))
    #print(str(n_w)+","+str(n_h))

    off_w = (n_w-size)/2
    off_h = (n_h-size)/2

    #print(str(off_w)+","+str(off_h))

    crop_im = im2.crop((off_w,off_h,im2.size[0]-off_w,im2.size[1]-off_h))
    crop_im = crop_im.resize((size,size))
    crop_im.save(output_path, quality=100)
