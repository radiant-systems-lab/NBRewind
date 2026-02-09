from sqlite3 import connect, OperationalError

class MetadataHandler:

    def __init__(self, path):
        self.path = path
        self.create_metadata_table()
    
    def create_metadata_table(self):
    # create a metadata table in an sqlite database
        try:
            connection = connect(f'{self.path}/metadata.db')
            cursor = connection.cursor()

            # Create table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    c_id INTEGER,
                    code TEXT NOT NULL,
                    parent_cid INTEGER NOT NULL,
                    PRIMARY KEY (c_id, parent_cid)
                )
            ''')
            self.init = True
            connection.commit()
        except OperationalError as e:
            print(f"An error occurred: {e}")
        finally:
            connection.close()

    def get_checkpoint(self, code, last_cid):
        # Check if a checkpoint exists and retrieve outputs from the SQLite database
        try:
            connection = connect(f'{self.path}/metadata.db')
            cursor = connection.cursor()

            # Query the database for the checkpoint and its outputs
            cursor.execute('''
                SELECT c_id FROM metadata WHERE code = ? AND parent_cid = ?
            ''', (code, last_cid))
            result = cursor.fetchone()

            if result:
                return result[0]
                # return c_id, outputs  # outputs will be a JSON string (or None)
            return None
        except OperationalError as e:
            print(f"An error occurred: {e}")
            return None
        finally:
            connection.close()
    
    # def get_checkpoint(self, code, last_cid):
    #     # check if a checkpoint exists in an sqllite database
    #     try:
    #         connection = connect(f'{self.path}/metadata.db')
    #         cursor = connection.cursor()
            
    #         # Query the database for the existence of the checkpoint
    #         cursor.execute('''
    #             SELECT c_id FROM metadata WHERE code = ? AND parent_cid = ?
    #         ''', (code, last_cid))
    #         result = cursor.fetchone()
            
    #         if result:
    #             # If a checkpoint exists, return the c_id
    #             return result[0]
    #         return
    #     except OperationalError as e:
    #         print(f"An error occurred: {e}")
    #         return False
    #     finally:
    #         connection.close()

    # def get_metadata_from_db(self, code, execution_id=None):
    #     # retrieve metadata from an sqllite database
    #     try:
    #         connection = connect(f'{self.path}/metadata.db')
    #         cursor = connection.cursor()
            
    #         # Query the database for metadata
    #         cursor.execute('''
    #             SELECT execution_id, code FROM metadata WHERE code = ? and execution_id = ?
    #         ''', (code, execution_id))
    #         result = cursor.fetchone()
            
    #         if result:
    #             self.execution_id, self.code = result
    #             return self.execution_id, self.code
    #         else:
    #             return None, None
    #     except OperationalError as e:
    #         print(f"An error occurred: {e}")
    #         return None, None
    #     finally:
    #         connection.close()

    def persist_metadata(self, code, parent_cid):
        
        # persist metadata to an sqllite database
        # create a connection to the database
        try:
            connection = connect(f'{self.path}/metadata.db')
            cursor = connection.cursor()
            
            #  Get the next c_id
            cursor.execute("SELECT COALESCE(MAX(c_id), 0) + 1 FROM metadata")
            next_id = cursor.fetchone()[0]

            # Insert row with new c_id
            cursor.execute("INSERT INTO metadata (c_id, code, parent_cid) VALUES (?, ?, ?)", 
                        (next_id, code, parent_cid))
            
            # # Insert metadata
            # cursor.execute('''
            #     INSERT INTO metadata (code, parent_cid) VALUES (?, ?)
            # ''', (code, parent_cid))
            
            # Commit changes and close the connection
            connection.commit()
            return next_id 
        except OperationalError as e:
            print(f"An error occurred: {e}")
        finally:
            connection.close()

    def get_next_checkpoints(self, last_ran_cid):
        # retrieve all c_id whose parent is the last_ran_cid
        try:
            connection = connect(f'{self.path}/metadata.db')
            cursor = connection.cursor()
            
            # Query the database for all c_id with the given parent_cid
            cursor.execute('''
                SELECT c_id, code FROM metadata WHERE parent_cid = ?
            ''', (last_ran_cid,))
            results = cursor.fetchall()
            
            # Return a dictionary of c_ids and codes
            return {row[0]: row[1] for row in results}
        except OperationalError as e:
            print(f"An error occurred: {e}")
            return {}
        finally:
            connection.close()

    def get_all_checkpoints(self):
        # retrieve all c_id
        try:
            connection = connect(f'{self.path}/metadata.db')
            cursor = connection.cursor()
            
            # Query the database for all c_id with the given parent_cid
            cursor.execute('''
                SELECT c_id, code FROM metadata 
            ''')
            results = cursor.fetchall()
            
            # Return a dictionary of c_ids and codes
            return {row[0]: row[1] for row in results}
        except OperationalError as e:
            print(f"An error occurred: {e}")
            return {}
        finally:
            connection.close()