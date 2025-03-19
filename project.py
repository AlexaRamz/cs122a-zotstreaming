import sys
import os
import csv

# Using MySQL with Python: https://www.w3schools.com/python/python_mysql_getstarted.asp
import mysql.connector

db = mysql.connector.connect(
    user="test",
    password="password",
    database="cs122a"
)

# Executing SQL queries in Python: https://www.w3schools.com/python/python_mysql_select.asp
cursor = db.cursor()


def import_data(folder_name: str) -> bool:
    """Delete existing tables, and create new tables. Then read the csv files in the given folder and import data into the database. You can assume that the folder always contains all the necessary CSV files and the files are correct.

        Args:
                        folderName (str): Folder containing csv files to read

        Returns:
                        bool: Whether the action was successful
    """
    # Delete existing database and create new tables by running DDL statements
    instructions_file_path = "create_tables_instructions.txt"
    try:
        with open(instructions_file_path, "r") as file:
            sql_script = file.read()
            sql_statements = [s.strip() for s in sql_script.split(";")]
            for statement in sql_statements:
                cursor.execute(statement)
            db.commit()
    except FileNotFoundError:
        print(f"Error: The file {instructions_file_path} was not found.")
        return False

    # Read the csv files and import data
    # https://www.geeksforgeeks.org/reading-csv-files-in-python/
    for table_name in ("users", "producers", "viewers", "releases", "movies", "series", "videos", "sessions", "reviews"):
        file_name = table_name + ".csv"
        file_path = os.path.join(folder_name, file_name)
        table_name = table_name.capitalize()

        try:
            with open(file_path, "r") as file:
                csv_file = csv.reader(file)
                column_names = next(csv_file)
                for row in csv_file:
                    placeholders = ", ".join(["%s"] * len(row))
                    sql_command = f"""
                    INSERT INTO {table_name} ({", ".join(column_names)})
                    VALUES ({placeholders});
                    """
                    try:
                        cursor.execute(sql_command, row)
                    except mysql.connector.Error as e:
                        # print(f"Error inserting record {row} into table {table_name}: {e}")
                        return False
                db.commit()
        except FileNotFoundError:
            # print(f"Error: The file {file_path} was not found.")
            return False

    return True


def insert_viewer(uid: int, email: str, nickname: str, street: str, city: str, state: str, zip: str, genres: str, joined_date: str, first_name: str, last_name: str, subscription: str) -> bool:
    """Insert a new viewer into the related tables.

    Returns:
        bool: Whether the action was successful
    """

    # Insert into: https://www.w3schools.com/python/python_mysql_insert.asp
    # Insert record into Users table
    user_statement = """
    INSERT INTO Users (uid, email, joined_date, nickname, street, city, state, zip, genres)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    user_values = (uid, email, joined_date, nickname,
                   street, city, state, zip, genres)
    try:
        cursor.execute(user_statement, user_values)
    except mysql.connector.Error as e:
        # print(f"Error inserting record into Users: {e}")
        return False

    # Inser record into Viewers table
    viewer_statement = """
    INSERT INTO Viewers (uid, subscription, first_name, last_name)
    VALUES (%s, %s, %s, %s)
    """
    viewer_values = (uid, subscription, first_name, last_name)
    try:
        cursor.execute(viewer_statement, viewer_values)
    except mysql.connector.Error as e:
        # print(f"Error inserting record into Viewers: {e}")
        return False

    db.commit()
    return True


def add_genre(uid: int, genre: str) -> bool:
    """Add a new genre to a user. For purposes of this homework, the genres attribute is a list of semicolon separated words. If the user already has existing genres (e.g. "comedy;romance"), the new genre should be added to the semicolon-separated list (e.g. for a new genre "horror", the genre column will be updated to "comedy;romance;horror".

    Returns:
    bool: Whether the action was successful
    """
    # Get existing genre list for the user, if any
    # Select from: https://www.w3schools.com/python/python_mysql_select.asp
    get_genres_statement = f"""
    SELECT genres
    FROM Users
    WHERE uid = {uid};
    """
    try:
        cursor.execute(get_genres_statement)
        result = cursor.fetchone()
    except mysql.connector.Error as e:
        # print(f"Error retrieving genres for record with uid {uid}: {e}")
        return False
    
    genres = genre
    if result is not None:
        genre_list = result[0].split(";")
        if genre not in genre_list:
            genre_list.append(genre)  # Add the specified genre
            genres = ";".join(genre_list)
        else:
            # print(f"User with uid {uid} already has genre {genre}")
            return False

    # Update genre list for the user
    update_genres_statement = f"""
    UPDATE Users
    SET genres = "{genres}"
    WHERE uid = {uid};
    """
    try:
        cursor.execute(update_genres_statement)
        db.commit()
    except mysql.connector.Error as e:
        # print(f"Error updating genres for record with uid {uid}: {e}")
        return False

    return True


def delete_viewer(uid: int) -> bool:
    """Given a Viewer uid, delete the Viewer from the appropriate table(s).

    Returns:
    bool: Whether the action was successful
    """
    sql_statement = f"""
    DELETE FROM Viewers
    WHERE uid = {uid};
    """
    try:
        cursor.execute(sql_statement)
        db.commit()
    except mysql.connector.Error as e:
        # print(f"Error removing record with uid {uid} from Viewers: {e}")
        return False

    return True


def insert_movie(rid: int, website_url: str) -> bool:
    """Insert a new movie in the appropriate table(s). Assume that the corresponding Release record already exists."""
    

    """Checks if the movie rid is already in the Releases table"""
    releases_statement = f"""SELECT rid FROM Releases WHERE rid = {rid};"""

    try:
        cursor.execute(releases_statement)
        result = cursor.fetchone()
    except mysql.connector.Error as e:
        return False
    
    """If no movie is found with the same rid, add it to the table"""
    if result is None:
        movie_sql_statement = f"""INSERT INTO Movies(rid, website_url) VALUES (%s, %s);"""
        movie_val = (rid, website_url)

        try: 
            cursor.execute(movie_sql_statement, movie_val)
        
        except mysql.connector.Error as e:
            return False
    db.commit()
    return True


def insert_session(sid: int, uid: int, rid: int, ep_num: int, initiate_at: str, leave_at: str, quality: str, device: str) -> bool:
    """Insert a new session that was played by a specific viewer which streamed a specific video.

    Returns:
    bool: Whether the action was successful
    """

    """Insert new session into the Sessions Table"""
    session_statement = f"""INSERT INTO Sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
    session_values = (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device)

    try:
        cursor.execute(session_statement, session_values)
        db.commit()
        return True

    except mysql.connector.Error as e:
        return False


def update_release(rid: int, title: str) -> bool:
    """Update the title of a release

    Returns:
    bool: Whether the action was successful
    """
    """Updates the title on the Release Table"""
    update_release = f"""UPDATE Releases SET title = %s WHERE rid = %s;"""

    try:
        cursor.execute(update_release, (title, rid))
        db.commit()
        return True
    
    except mysql.connector.Error as e:
        return False
    
    """Updates the title in the Videos table??? Not sure if the title is different to the one in Releases"""


def list_releases(uid: int) -> None:
    """Given a viewer ID, list all the unique releases the viewer has reviewed in ASCENDING order on the release title

    Returns:
    Table - rid, genre, title
    """
    """Might have to change sql statement below"""
    get_release_statement = f"""SELECT DISTINCT r.rid, r.genre, r.title FROM Releases as r JOIN Reviews as review ON review.rid = r.rid WHERE review.uid = %s ORDER BY title ASC;"""
    
    try:
        cursor.execute(get_release_statement, (uid, ))
        result = cursor.fetchall()
        #print("here")
        """If the output is a result table, print each record in one line and separate columns with ‘,’ - just like the format of the dataset file. """
        for i in result:
            print(f"{i[0]},{i[1]},{i[2]}")
    
    except mysql.connector.Error as e:
        pass


def list_popular_releases(N: int):
    """List the top N releases that have the most reviews, in DESCENDING order on reviewCount, rid.

    Returns:
    Table - rid, title, reviewCount
    """
    get_popular_releases = f"""
        WITH RevCounts AS (
            SELECT rid, COUNT(*) as reviewCount
            FROM Reviews
            GROUP BY rid)
        SELECT r.rid, r.title, COALESCE(rc.reviewCount, 0) as reviewCount
        FROM Releases r LEFT JOIN RevCounts rc ON r.rid = rc.rid
        ORDER BY reviewCount DESC, rid DESC
        LIMIT {N}
        """
    
    try:
        cursor.execute(get_popular_releases)
        result = cursor.fetchall()
        
        for i in result:
            print(f"{i[0]},{i[1]},{i[2]}")
    
    except mysql.connector.Error as e:
        print(f"Error: {e}")


def get_release_title(sid: int):
    """Given a session ID, find the release associated with the video streamed in the session. List information on both the release and video, in ASCENDING order on release title. 

    Returns:
    Table - rid, release_title, genre, video_title, ep_num, length
    """
    get_release_info = f"""
        WITH RelVids AS (SELECT r.rid,
                                r.title AS release_title,
                                r.genre,
                                v.ep_num,
                                v.title AS video_title,
                                v.length
              FROM Releases r
              JOIN Videos v ON r.rid = v.rid)
        SELECT rid, release_title, genre, video_title, ep_num, length
        FROM Sessions NATURAL JOIN RelVids
        WHERE sid = {sid}
        ORDER BY release_title ASC
        """
        
    try:
        cursor.execute(get_release_info)
        result = cursor.fetchall()
        
        for i in result:
            print(f"{i[0]},{i[1]},{i[2]},{i[3]},{i[4]},{i[5]}")
    
    except mysql.connector.Error as e:
        print(f"Error: {e}")


def list_active_viewers(N: int, start: str, end: str):
    """Find all active viewers that have started a session more than N times (including N) in a specific time range (including start and end date), in ASCENDING order by uid. N will be at least 1.

    Returns:
    Table - UID, first name, last name
    """
    pass


def get_videos_viewed(rid: int):
    """Given a Video rid, count the number of unique viewers that have started a session on it. Videos that are not streamed by any viewer should have a count of 0 instead of NULL. Return video information along with the count in DESCENDING order by rid.

    Returns:
    Table - RID, ep_num, title, length,COUNT
    """
    pass


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        args = []

        # Get only the arguments for the function (exclude script name and function name)
        if len(sys.argv) > 2:
            args = sys.argv[2:] 
        
        # Process arguments
        for i in range(len(args)):
            args[i] = args[i].strip("\"") # Remove quotations for strings that may have spaces
            if args[i] == "NULL":
                args[i] = None # Replace "NULL" with None
            
        result = None
        try:
            if cmd == "import":
                result = import_data(*args)
            elif cmd == "insertViewer":
                args[0] = int(args[0])
                result = insert_viewer(*args)
            elif cmd == "addGenre":
                args[0] = int(args[0])
                result = add_genre(*args)
            elif cmd == "deleteViewer":
                args[0] = int(args[0])
                result = delete_viewer(*args)
            elif cmd == "insertMovie":
                args[0] = int(args[0])
                result = insert_movie(*args)
            elif cmd == "insertSession":
                args[0] = int(args[0])
                args[1] = int(args[1])
                args[2] = int(args[2])
                args[3] = int(args[3])

                result = insert_session(*args)
            elif cmd == "updateRelease":
                args[0] = int(args[0])
                result = update_release(*args)
            elif cmd == "listReleases":
                args[0] = int(args[0])
                result = list_releases(*args)
            elif cmd == "popularRelease":
                args[0] = int(args[0])
                result = list_popular_releases(*args)
            elif cmd == "releaseTitle":
                args[0] = int(args[0])
                result = get_release_title(*args)
            elif cmd == "activeViewer":
                args[0] = int(args[0])
                result = list_active_viewers(*args)
            elif cmd == "videosViewed":
                args[0] = int(args[0])
                result = get_videos_viewed(*args)
            else:
                print("Command not found")

            if type(result) is bool:
                if result:
                    print("Success")
                else:
                    print("Fail")

        except ValueError as e:
            print(f"Wrong type for one or more arguments: ", {e})
        except TypeError as e:
            print(
                f"Invalid number of arguments, or wrong type for one or more arguments: ", {e})
        except IndexError as e:
            print(f"Invalid number of arguments: ", {e})
