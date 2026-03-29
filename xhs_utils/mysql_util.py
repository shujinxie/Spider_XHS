import json
import pymysql
from loguru import logger


def get_mysql_connection(mysql_config: dict):
    return pymysql.connect(
        host=mysql_config['host'],
        port=int(mysql_config.get('port', 3306)),
        user=mysql_config['user'],
        password=mysql_config['password'],
        database=mysql_config['database'],
        charset='utf8mb4',
        autocommit=False,
    )


def init_mysql_tables(conn):
    note_table_sql = """
    CREATE TABLE IF NOT EXISTS xhs_notes (
        note_id VARCHAR(64) PRIMARY KEY,
        note_url VARCHAR(512),
        note_type VARCHAR(32),
        user_id VARCHAR(64),
        home_url VARCHAR(512),
        nickname VARCHAR(255),
        avatar VARCHAR(1024),
        title TEXT,
        `desc` MEDIUMTEXT,
        liked_count BIGINT,
        collected_count BIGINT,
        comment_count BIGINT,
        share_count BIGINT,
        video_cover VARCHAR(1024),
        video_addr VARCHAR(1024),
        image_list_json MEDIUMTEXT,
        tags_json MEDIUMTEXT,
        upload_time DATETIME,
        ip_location VARCHAR(255),
        keyword_hits_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    comment_table_sql = """
    CREATE TABLE IF NOT EXISTS xhs_comments (
        comment_id VARCHAR(64) PRIMARY KEY,
        note_id VARCHAR(64) NOT NULL,
        note_url VARCHAR(512),
        user_id VARCHAR(64),
        home_url VARCHAR(512),
        nickname VARCHAR(255),
        avatar VARCHAR(1024),
        content MEDIUMTEXT,
        show_tags_json TEXT,
        like_count BIGINT,
        upload_time DATETIME,
        ip_location VARCHAR(255),
        pictures_json MEDIUMTEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_note_id (note_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with conn.cursor() as cursor:
        cursor.execute(note_table_sql)
        cursor.execute(comment_table_sql)
    conn.commit()


def save_notes_and_comments_to_mysql(notes, comments, mysql_config: dict):
    conn = get_mysql_connection(mysql_config)
    try:
        init_mysql_tables(conn)
        with conn.cursor() as cursor:
            note_sql = """
            INSERT INTO xhs_notes (
                note_id, note_url, note_type, user_id, home_url, nickname, avatar, title, `desc`,
                liked_count, collected_count, comment_count, share_count, video_cover, video_addr,
                image_list_json, tags_json, upload_time, ip_location, keyword_hits_json
            ) VALUES (
                %(note_id)s, %(note_url)s, %(note_type)s, %(user_id)s, %(home_url)s, %(nickname)s, %(avatar)s, %(title)s, %(desc)s,
                %(liked_count)s, %(collected_count)s, %(comment_count)s, %(share_count)s, %(video_cover)s, %(video_addr)s,
                %(image_list_json)s, %(tags_json)s, %(upload_time)s, %(ip_location)s, %(keyword_hits_json)s
            )
            ON DUPLICATE KEY UPDATE
                note_url=VALUES(note_url), note_type=VALUES(note_type), user_id=VALUES(user_id), home_url=VALUES(home_url),
                nickname=VALUES(nickname), avatar=VALUES(avatar), title=VALUES(title), `desc`=VALUES(`desc`),
                liked_count=VALUES(liked_count), collected_count=VALUES(collected_count), comment_count=VALUES(comment_count),
                share_count=VALUES(share_count), video_cover=VALUES(video_cover), video_addr=VALUES(video_addr),
                image_list_json=VALUES(image_list_json), tags_json=VALUES(tags_json), upload_time=VALUES(upload_time),
                ip_location=VALUES(ip_location), keyword_hits_json=VALUES(keyword_hits_json);
            """

            comment_sql = """
            INSERT INTO xhs_comments (
                comment_id, note_id, note_url, user_id, home_url, nickname, avatar, content,
                show_tags_json, like_count, upload_time, ip_location, pictures_json
            ) VALUES (
                %(comment_id)s, %(note_id)s, %(note_url)s, %(user_id)s, %(home_url)s, %(nickname)s, %(avatar)s, %(content)s,
                %(show_tags_json)s, %(like_count)s, %(upload_time)s, %(ip_location)s, %(pictures_json)s
            )
            ON DUPLICATE KEY UPDATE
                note_id=VALUES(note_id), note_url=VALUES(note_url), user_id=VALUES(user_id), home_url=VALUES(home_url),
                nickname=VALUES(nickname), avatar=VALUES(avatar), content=VALUES(content), show_tags_json=VALUES(show_tags_json),
                like_count=VALUES(like_count), upload_time=VALUES(upload_time), ip_location=VALUES(ip_location), pictures_json=VALUES(pictures_json);
            """

            for note in notes:
                payload = note.copy()
                payload['image_list_json'] = json.dumps(payload.get('image_list', []), ensure_ascii=False)
                payload['tags_json'] = json.dumps(payload.get('tags', []), ensure_ascii=False)
                payload['keyword_hits_json'] = json.dumps(payload.get('keyword_hits', []), ensure_ascii=False)
                cursor.execute(note_sql, payload)

            for comment in comments:
                payload = comment.copy()
                payload['show_tags_json'] = json.dumps(payload.get('show_tags', []), ensure_ascii=False)
                payload['pictures_json'] = json.dumps(payload.get('pictures', []), ensure_ascii=False)
                cursor.execute(comment_sql, payload)

        conn.commit()
        logger.info(f"MySQL 保存成功，笔记 {len(notes)} 条，评论 {len(comments)} 条")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
