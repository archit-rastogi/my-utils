from sentence_transformers import SentenceTransformer
from torch import Tensor
import psycopg2
from typing import List
import traceback

from threading import Thread
import threading

# You might need to choose a model that natively outputs 1536 dimensions
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')


def generate_embedding(corpus) -> List[float]:
    embedding: Tensor = model.encode(corpus)
    return embedding.tolist()


def update_vector(batch_size, offset, bound):
    with psycopg2.connect(host="172.165.19.37,172.165.44.136,172.165.54.186",
                          port=5433,
                          database="yb1",
                          user="yugabyte",
                          password="") as conn:
        conn.autocommit = False
        curr = offset
        while curr < bound:
            update_vector_partsupp(conn, batch_size, curr)
            curr += batch_size


def update_vector_partsupp(conn: psycopg2.extensions.connection, batch_size: int, offset: int):
    cursor:  psycopg2.extensions.cursor
    ident = threading.get_ident()
    temp_tbl_name = f"temp_{ident}"
    with conn.cursor() as cursor:
        try:
            select_query = f"""select ps_partkey, ps_suppkey, ps_comment
                from partsupp for update offset {offset} limit {batch_size}"""
            cursor.execute(select_query)
            result = cursor.fetchall()
            rows_to_update = []
            # generate embeddings
            for row in result:
                embedding = generate_embedding(row[-1])
                # insert into a temp table
                rows_to_update.append(f"({row[0]}, {row[1]}, '{embedding}'::vector(384))")
            ins_str = ",".join(rows_to_update)
            temp_table = f"CREATE TEMP TABLE {temp_tbl_name}(ps_partkey, ps_suppkey, vec) AS VALUES {ins_str}"
            # print(temp_table)
            cursor.execute(temp_table)

            update_query = f"""update partsupp set vec = t1.vec
                from (select ps_partkey, ps_suppkey, vec::vector(384) from {temp_tbl_name}) AS t1
                where partsupp.ps_partkey = t1.ps_partkey  and partsupp.ps_suppkey = t1.ps_suppkey"""
            cursor.execute(update_query)
            conn.commit()
            print(f"{ident}: updated {batch_size} vectors starting {offset}")
            return
        except Exception:
            print(traceback.format_exc())
            conn.rollback()
        finally:
            cursor.execute(f"drop table if exists {temp_tbl_name};")


if __name__ == "__main__":
    threads = []
    for i in range(0, 10):
        step_size = 80000
        print((i * step_size, (i+1) * step_size))
        kwargs = {
            "batch_size": 1000,
            "offset": i * step_size,
            "bound": (i+1) * step_size
        }
        thread = Thread(target=update_vector, kwargs=kwargs)
        thread.start()
        threads.append(thread)

    for th in threads:
        th.join()
