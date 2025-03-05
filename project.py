import sys

def import_data(folderName: str) -> bool:
	"""Delete existing tables, and create new tables. Then read the csv files in the given folder and import data into the database. You can assume that the folder always contains all the necessary CSV files and the files are correct. 
	
	Args:
    folderName (str): Folder containing csv files to read
	
	Returns:
    bool: Whether the action was successful
  """
	pass

def insert_viewer(uid: int, email: str, nickname: str, street: str, city: str, state: str, zip:str, genres:str, joined_date: str, first:str, last:str, subscription:str) -> bool:
	"""Insert a new viewer into the related tables.

	Returns:
    bool: Whether the action was successful
  """
	pass

def add_genre(uid: int, genre: str) -> bool:
	"""Add a new genre to a user. For purposes of this homework, the genres attribute is a list of semicolon separated words. If the user already has existing genres (e.g. "comedy;romance"), the new genre should be added to the semicolon-separated list (e.g. for a new genre "horror", the genre column will be updated to "comedy;romance;horror".

	Returns:
    bool: Whether the action was successful
  """
	pass

def delete_viewer(uid: int) -> bool:
	"""Given a Viewer uid, delete the Viewer from the appropriate table(s).

	Returns:
    bool: Whether the action was successful
  """
	pass

def insert_movie(rid: int, website_url: str) -> bool:
	"""Insert a new movie in the appropriate table(s). Assume that the corresponding Release record already exists.

	Returns:
    bool: Whether the action was successful
  """
	pass

def insert_session(sid: int, uid: int, rid: int, ep_num: int, initiate_at: str, leave_at: str, quality:str, device:str) -> bool:
	"""Insert a new session that was played by a specific viewer which streamed a specific video.

	Returns:
    bool: Whether the action was successful
  """
	pass

def update_release(rid: int, title: str) -> bool:
	"""Update the title of a release

	Returns:
    bool: Whether the action was successful
  """
	pass

def list_releases(uid: int) -> bool:
	"""Given a viewer ID, list all the unique releases the viewer has reviewed in ASCENDING order on the release title

	Returns:
    Table - rid, genre, title
  """
	pass

def list_popular_releases(N: int):
	"""List the top N releases that have the most reviews, in DESCENDING order on reviewCount, rid.

	Returns:
    Table - rid, title, reviewCount
  """
	pass

def get_release_title(sid: int):
	"""Given a session ID, find the release associated with the video streamed in the session. List information on both the release and video, in ASCENDING order on release title. 

	Returns:
    Table - rid, release_title, genre, video_title, ep_num, length
  """
	pass
	
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
    
    if len(sys.argv) > 2:
      args = sys.argv[2:]
		
    try: 
      if cmd == "import":
        print(import_data(*args))
      elif cmd == "insertViewer":
        args[0] = int(args[0])
        print(insert_viewer(*args))
      elif cmd == "addGenre":
        args[0] = int(args[0])
        print(add_genre(*args))
      elif cmd == "deleteViewer":
        args[0] = int(args[0])
        print(delete_viewer(*args))
      elif cmd == "insertMovie":
        args[0] = int(args[0])
        print(insert_movie(*args))
      elif cmd == "insertSession":
        args[0] = int(args[0])
        args[1] = int(args[1])
        args[2] = int(args[2])
        args[3] = int(args[3])
        print(insert_session(*args))
      elif cmd == "updateRelease":
        args[0] = int(args[0])
        print(update_release(*args))
      elif cmd == "listReleases":
        args[0] = int(args[0])
        print(list_releases(*args))
      elif cmd == "popularRelease":
        args[0] = int(args[0])
        print(list_popular_releases(*args))
      elif cmd == "releaseTitle":
        args[0] = int(args[0])
        print(get_release_title(*args))
      elif cmd == "activeViewer":
        args[0] = int(args[0])
        print(list_active_viewers(*args))
      elif cmd == "videosViewed":
        args[0] = int(args[0])
        print(get_videos_viewed(*args))
      
    except ValueError as e:
      print(f"Wrong type for one or more arguments: ", {e})
    except TypeError as e:
      print(f"Invalid number of arguments, or wrong type for one or more arguments: ", {e})
    except IndexError as e:
      print(f"Invalid number of arguments: ", {e})
