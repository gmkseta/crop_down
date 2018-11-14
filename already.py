import urllib.request
import os
import time
import pymysql
import pdb;
import sys;
from crop import crop_img
from env_setting import host, user, password, db

# DOWNLOAD_PATH = sys.argv[1]
CROP_PATH = sys.argv[1]


# 각각의 카테고리 마다 몇개 다운받았는지 DB에 저장
class ImageDownloader:
    def __init__(self):
        self.get_connection()
        self.sharding_no = '0/'
        self.index_no = 0
        # ong = sorted(glob.glob(CROP_PATH+"/*"))

    def get_connection(self):
        is_conn_success = False
        while not is_conn_success:
            try:
                self.conn = pymysql.connect(host=host,
                                            user=user,
                                            password=password,
                                            db=db,
                                            charset='utf8',
                                            cursorclass=pymysql.cursors.DictCursor)
            except Exception as e:
                print("db connection exception occures")
                print(e)
                continue

            if self.conn is not None:
                is_conn_success = True

        return self.conn

    def disconnect_connection(self):
        self.conn.close()

    def __del__(self):
        self.disconnect_connection()

    def get_all_urls(self, size=1000,offset=0):
        get_all_url_sql = 'SELECT image_idx, image_url, file_address FROM image_info WHERE status = 1 LIMIT %s OFFSET %s'
        result = list()

        read_success = False
        while not read_success:
            try:
                conn = self.conn
                cursor = conn.cursor()

                cursor.execute(get_all_url_sql, (size,offset))

                result = cursor.fetchall()

            except Exception as e:
                print(e)
                continue
            finally:
                cursor.close()

            read_success = True

        return result

    def get_specific_urls(self, keyword, size=1000):
        get_url_sql = 'SELECT image_idx,file_address FROM image_info WHERE status = 1 and search_keyword = %s LIMIT %s'
        result = list()
        read_success = False

        while not read_success:
            try:
                conn = self.conn
                cursor = conn.cursor()

                cursor.execute(get_url_sql, (keyword, size))

                result = cursor.fetchall()

            except Exception as e:
                print(e)
                continue
            finally:
                cursor.close()

            read_success = True

        return result

    def download_crop(self, addr_list):
        self.sharding_no = str(self.index_no // 1000) + "/"
        for url_info in addr_list:
            if url_info['image_idx'] is not None :
                file_address = url_info['file_address']
            else:
                continue

            # 파일명은 image_idx로 지정
            filename = str(self.index_no) + ".jpg"


            path = os.getcwd() + CROP_PATH + "/" + self.sharding_no

            file_path = path + filename

            # Create when directory does not exist
            if not os.path.isdir(path):
                os.makedirs(path)

            # download
            is_download_success = False
            try_count = 0


            while not is_download_success:
                try:
                    # download img using url
                    #pdb.set_trace()
                    crop_img(os.getcwd()+"/"+file_address,file_path,224)
                except Exception as e:
                    print(e)
                    # 5회 다운로드 시도 후 실패하면 다음 이미지로 넘어감
                    if try_count < 5:
                        print("download failed. try again...")
                        try_count = try_count + 1
                        continue
                    else:
                        break

                is_download_success = True
            # 폴더명과 파일 이름 지정

            if self.index_no%100==0: print("%s is downloading.. " %(file_path))
            self.sharding_no = str(self.index_no // 1000) + "/"
            self.index_no+=1

    def run_download(self, keyword="all", size=1000):
        offset=0
        while True:
            start_time = time.time()
            if keyword == "all":
                addr_list = self.get_all_urls(size,offset)
            else:
                addr_list = self.get_specific_urls(keyword, size)

            if len(addr_list) == 0:
                print('no url exists')
                break

            print("url list size : ", len(addr_list))

            self.download_crop(addr_list)

            print("download 1000 images took %s seconds" % (time.time() - start_time))
            offset+=size

        print("download finish")


if __name__ == "__main__":

    obj = ImageDownloader()
    obj.run_download(size=1000)
