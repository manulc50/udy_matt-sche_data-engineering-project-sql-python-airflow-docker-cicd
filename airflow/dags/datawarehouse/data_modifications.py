import logging

logger = logging.getLogger(__name__)

TABLE_NAME = "yt_api_videos"

def insert_row(connection, cursor, schema_name, row):
    try:
        if schema_name == 'staging':
            video_id = "video_id"

            cursor.execute(f"""INSERT INTO {schema_name}.{TABLE_NAME} ("id", "title", "upload_date", "duration", "views", "likes_count", "comments_count")
                        VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                        """, row)
        else:
            video_id = "id"

            cursor.execute(f"""INSERT INTO {schema_name}.{TABLE_NAME} ("id", "title", "upload_date", "duration", "type", "views", "likes_count", "comments_count")
                        VALUES (%(id)s, %(title)s, %(upload_date)s, %(duration)s, %(type)s, %(views)s, %(likes_count)s, %(comments_count)s);
                        """, row)
            
        # Realizamos un commit en la base de datos porque estamos realizando un cambio.
        connection.commit()

        logger.info(f"Inserted row with video_id: {row[video_id]}")
    
    except Exception as e:
        logger.error(f"Error inserting row with video_id: {row[video_id]} - {e}")
        raise e
    

def update_row(connection, cursor, schema_name, row):
    try:
        if schema_name == "staging":
            video_id = "video_id"
            video_title = "title"
            video_upload_date = "publishedAt"
            video_views = "viewCount"
            video_likes_count = "likeCount"
            video_comments_count = "commentCount"
        else:
            video_id = "id"
            video_title = "title"
            video_upload_date = "upload_date"
            video_views = "views"
            video_likes_count = "likes_count"
            video_comments_count = "comments_count"

        cursor.execute(f"""
                       UPDATE {schema_name}.{TABLE_NAME}
                       SET "title" = %({video_title})s,
                           "views" = %({video_views})s,
                           "likes_count" = %({video_likes_count})s,
                           "comments_count" = %({video_comments_count})s
                        WHERE "id" = %({video_id})s AND "upload_date" = %({video_upload_date})s;
                        """, row)
        
        # Realizamos un commit en la base de datos porque estamos realizando un cambio.
        connection.commit()

        logger.info(f"Updated row with video_id: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with video_id: {row[video_id]} - {e}")
        raise e


def delete_rows(connection, cursor, schema_name, ids_to_delete):
    try:
        ids_to_delete_str = f"({', '.join(ids_to_delete)})"

        cursor.execute(f"""
                       DELETE FROM {schema_name}.{TABLE_NAME}
                       WHERE "id" IN {ids_to_delete_str};
        """)

        # Realizamos un commit en la base de datos porque estamos realizando un cambio.
        connection.commit()

        logger.info(f"Deleted rows with video_ids: {ids_to_delete_str}")

    except Exception as e:
        logger.error(f"Error deleting rows with video_ids: {ids_to_delete_str} - {e}")
        raise e