/*
 * Copyright (C) 2022 GK Spencer
 *
 * JFileServer is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JFileServer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with JFileServer. If not, see <http://www.gnu.org/licenses/>.
 */

package org.filesys.server.filesys.db.h2;

import org.filesys.debug.Debug;
import org.filesys.server.config.InvalidConfigurationException;
import org.filesys.server.filesys.*;
import org.filesys.server.filesys.cache.FileState;
import org.filesys.server.filesys.db.*;
import org.filesys.server.filesys.loader.*;
import org.filesys.smb.server.ntfs.StreamInfo;
import org.filesys.smb.server.ntfs.StreamInfoList;
import org.filesys.util.MemorySize;
import org.filesys.util.StringList;
import org.filesys.util.WildCard;
import org.filesys.util.db.DBConnectionPool;
import org.filesys.util.db.DBStatus;
import org.springframework.extensions.config.ConfigElement;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

/**
 * H2 Database Interface Class
 *
 * <p>H2 specific implementation of the database interface used by the database filesystem
 * driver (DBDiskDriver).
 *
 * @author gkspencer
 */
public class H2SQLDBInterface extends JdbcDBInterface implements DBQueueInterface, DBDataInterface, DBObjectIdInterface {

    // Memory buffer maximum size
    public final static long MaxMemoryBuffer = MemorySize.MEGABYTE / 2; // 1/2Mb

    // Lock file name, used to check if server shutdown was clean or not
    public final static String LockFileName = "H2SQLLoader.lock";

    // OID read/write buffer size
    public final static int OIDBufferSize = 32 * (int) MemorySize.KILOBYTE;

    // File data fragment size, to be stored via an oid file
    public final static long OIDFileSize = MemorySize.GIGABYTE;

    // Database connection and prepared statement used to write file requests to the queue tables
    private Connection m_dbConn;
    private PreparedStatement m_reqStmt;
    private PreparedStatement m_tranStmt;

    /**
     * Default constructor
     */
    public H2SQLDBInterface() {
        super();
    }

    /**
     * Return the database interface name
     *
     * @return String
     */
    public String getDBInterfaceName() {
        return "H2SQL";
    }

    /**
     * Get the supported database features mask
     *
     * @return EnumSet&lt;Feature&gt;
     */
    protected EnumSet<Feature> getSupportedFeatures() {

        // Determine the available database interface features
        EnumSet<Feature> supFeatures = EnumSet.of(Feature.NTFS, Feature.Retention, Feature.SymLinks, Feature.Queue, Feature.Data);
        supFeatures.add(Feature.JarData);
        supFeatures.add(Feature.ObjectId);

        return supFeatures;
    }

    /**
     * Initialize the database interface
     *
     * @param dbCtx  DBDeviceContext
     * @param params ConfigElement
     * @throws InvalidConfigurationException Invalid configuration
     */
    public void initializeDatabase(DBDeviceContext dbCtx, ConfigElement params)
            throws InvalidConfigurationException {

        // Set the JDBC driver class, must be set before the connection pool is created
        setDriverName("org.h2.Driver");

        // Call the base class to do the main initialization
        super.initializeDatabase(dbCtx, params);

        // Set the fragment size for saving data to the database oid files
        setDataFragmentSize(OIDFileSize);

        // Create the database connection pool
        try {
            createConnectionPool();

            // Check if we should wait for a database connection before continuing
            if (hasStartupWaitForConnection()) {

                // Wait for a valid database connection
                if (!getConnectionPool().waitForConnection(getStartupWaitForConnection()))
                    throw new Exception("Failed to get database connection during startup wait time (" + getStartupWaitForConnection() + "secs)");
                else if (Debug.EnableDbg && hasDebug())
                    Debug.println("[H2] Startup wait for database connection successful");
            }
        } catch (Exception ex) {
            // DEBUG
            logException("[H2] Error creating connection pool: ", ex);

            // Rethrow the exception
            throw new InvalidConfigurationException("Failed to create connection pool, " + ex.getMessage());
        }

        // Check if the file system table exists
        Connection conn = null;

        try {
            // Open a connection to the database
            conn = getConnection();

            DatabaseMetaData dbMeta = conn.getMetaData();

            boolean foundStruct = false;
            boolean foundStream = false;
            boolean foundRetain = false;
            boolean foundQueue = false;
            boolean foundTrans = false;
            boolean foundData = false;
            boolean foundJarData = false;
            boolean foundObjId = false;
            boolean foundSymLink = false;

            // FIXED no result: Set the tableNamPattern (3rd param) to wildcard = % (or) null, not "" (empty not return a result)
            try (ResultSet rs = dbMeta.getTables("", "", "%", null)) {

                while (rs.next()) {

                    // Get the table name
                    String tblName = rs.getString("TABLE_NAME");

                    // Check if we found the filesystem structure or streams table
                    if (tblName.equalsIgnoreCase(getFileSysTableName()))
                        foundStruct = true;
                    else if (hasStreamsTableName() && tblName.equalsIgnoreCase(getStreamsTableName()))
                        foundStream = true;
                    else if (hasRetentionTableName() && tblName.equalsIgnoreCase(getRetentionTableName()))
                        foundRetain = true;
                    else if (hasDataTableName() && tblName.equalsIgnoreCase(getDataTableName()))
                        foundData = true;
                    else if (hasJarDataTableName() && tblName.equalsIgnoreCase(getJarDataTableName()))
                        foundJarData = true;
                    else if (hasQueueTableName() && tblName.equalsIgnoreCase(getQueueTableName()))
                        foundQueue = true;
                    else if (hasTransactionTableName() && tblName.equalsIgnoreCase(getTransactionTableName()))
                        foundTrans = true;
                    else if (hasObjectIdTableName() && tblName.equalsIgnoreCase(getObjectIdTableName()))
                        foundObjId = true;
                    else if (hasSymLinksTableName() && tblName.equalsIgnoreCase(getSymLinksTableName()))
                        foundSymLink = true;
                }
            }

            // Check if the file system structure table should be created
            if (!foundStruct) {

                // Create the file system structure table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS "
                            + getFileSysTableName()
                            + " (FileId IDENTITY, DirId BIGINT, FileName VARCHAR_IGNORECASE(255) NOT NULL, FileSize BIGINT,"
                            + "CreateDate BIGINT, ModifyDate BIGINT, AccessDate BIGINT, ChangeDate BIGINT, ReadOnly BOOLEAN, Archived BOOLEAN, Directory BOOLEAN,"
                            + "SystemFile BOOLEAN, Hidden BOOLEAN, IsSymLink BOOLEAN, Uid INTEGER, Gid INTEGER, Mode INTEGER, Deleted BOOLEAN NOT NULL DEFAULT FALSE, "
                            + "Encrypted BOOLEAN NOT NULL DEFAULT FALSE, PRIMARY KEY (FileId));");

                    // Create various indexes
                    stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS FileSysIFileDirId ON " + getFileSysTableName() + " (FileName,DirId);");
                    stmt.execute("CREATE INDEX IF NOT EXISTS FileSysIDirId ON " + getFileSysTableName() + " (DirId);");
                    stmt.execute("CREATE INDEX IF NOT EXISTS FileSysIDir ON " + getFileSysTableName() + " (DirId,Directory);");
                    stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS FileSysIFileDirIdDir ON " + getFileSysTableName() + " (FileName,DirId,Directory);");
                }

                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getFileSysTableName());
            }

            // Check if the file streams table should be created
            if (isNTFSEnabled() && !foundStream && getStreamsTableName() != null) {

                // Create the file streams table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS "
                            + getStreamsTableName()
                            + " (StreamId IDENTITY, FileId BIGINT NOT NULL, StreamName VARCHAR_IGNORECASE(255) NOT NULL, StreamSize BIGINT,"
                            + "CreateDate BIGINT, ModifyDate BIGINT, AccessDate BIGINT, PRIMARY KEY (StreamId));");

                    // Create various indexes
                    stmt.execute("CREATE INDEX IF NOT EXISTS StreamsIFileId ON " + getStreamsTableName() + " (FileId);");
                }

                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getStreamsTableName());
            }

            // Check if the retention table should be created
            if (isRetentionEnabled() && !foundRetain && getRetentionTableName() != null) {

                // Create the retention period data table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS " + getRetentionTableName()
                            + " (FileId BIGINT NOT NULL, StartDate TIMESTAMP, EndDate TIMESTAMP,"
                            + "PurgeFlag TINYINT(1), PRIMARY KEY (FileId));");
                }
                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getRetentionTableName());
            }

            // Check if the file loader queue table should be created
            if (isQueueEnabled() && !foundQueue && getQueueTableName() != null) {

                // Create the request queue data table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS "
                            + getQueueTableName()
                            + " (FileId BIGINT NOT NULL, StreamId BIGINT NOT NULL, ReqType SMALLINT,"
                            + "SeqNo SERIAL, TempFile TEXT, VirtualPath TEXT, QueuedAt TIMESTAMP, Attribs VARCHAR(512), PRIMARY KEY (SeqNo));");
                    stmt.execute("CREATE INDEX IF NOT EXISTS QueueIFileId ON " + getQueueTableName() + " (FileId);");
                    stmt.execute("CREATE INDEX IF NOT EXISTS QueueIFileIdType ON " + getQueueTableName() + " (FileId, ReqType);");
                }
                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getQueueTableName());
            }

            // Check if the file loader transaction queue table should be created
            if (isQueueEnabled() && !foundTrans && getTransactionTableName() != null) {

                // Create the transaction request queue data table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS " + getTransactionTableName()
                            + " (FileId BIGINT NOT NULL, StreamId BIGINT NOT NULL,"
                            + "TranId INTEGER NOT NULL, ReqType SMALLINT, TempFile TEXT, VirtualPath TEXT, QueuedAt TIMESTAMP,"
                            + "Attribs VARCHAR(512), PRIMARY KEY (FileId,StreamId,TranId));");
                }

                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getTransactionTableName());
            }

            // Check if the file data table should be created
            if (isDataEnabled() && !foundData && hasDataTableName()) {

                // Create the file data table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS "
                            + getDataTableName()
                            + " (FileId BIGINT NOT NULL, StreamId BIGINT NOT NULL, FragNo INTEGER, FragLen INTEGER, Data BLOB, JarFile BOOLEAN, JarId INTEGER);");

                    stmt.execute("CREATE INDEX IF NOT EXISTS DataIFileStreamId ON " + getDataTableName() + " (FileId,StreamId);");
                    stmt.execute("CREATE INDEX IF NOT EXISTS DataIFileId ON " + getDataTableName() + " (FileId);");
                    stmt.execute("CREATE INDEX IF NOT EXISTS DataIFileIdFrag ON " + getDataTableName() + " (FileId,FragNo);");
                }

                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getDataTableName());
            }

            // Check if the Jar file data table should be created
            if (isJarDataEnabled() && !foundJarData && hasJarDataTableName()) {

                // Create the Jar file data table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS " + getJarDataTableName()
                            + " (JarId IDENTITY, Data BLOB, PRIMARY KEY (JarId));");
                }

                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getJarDataTableName());
            }

            // Check if the file id/object id mapping table should be created

            if (isObjectIdEnabled() && !foundObjId && hasObjectIdTableName()) {

                // Create the file id/object id mapping table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS "
                            + getObjectIdTableName()
                            + " (FileId BIGINT NOT NULL, StreamId BIGINT NOT NULL, ObjectId VARCHAR(128), PRIMARY KEY (FileId,StreamId))");
                }

                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getObjectIdTableName());
            }

            // Check if the symbolic links table should be created
            if (isSymbolicLinksEnabled() && !foundSymLink && hasSymLinksTableName()) {

                // Create the symbolic links table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE IF NOT EXISTS " + getSymLinksTableName()
                            + " (FileId BIGINT NOT NULL PRIMARY KEY, SymLink VARCHAR(8192))");
                }

                // DEBUG
                if (Debug.EnableInfo && hasDebug())
                    Debug.println("[H2] Created table " + getSymLinksTableName());
            }
        } catch (Exception ex) {
            // DEBUG
            logException("[H2] Error on [initializeDatabase]: ", ex);
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
           //     closeConnection(conn);
            }
        }
    }

    /**
     * Check if a file/folder exists
     *
     * @param dirId int
     * @param fname String
     * @return FileStatus
     * @throws DBException Database error
     */
    public FileStatus fileExists(int dirId, String fname)
            throws DBException {

        // Check if the file exists, and whether it is a file or folder
        FileStatus sts = FileStatus.NotExist;

        Connection conn = null;

        try {
            // Get a connection to the database, create a statement for the database lookup
            conn = getConnection();

            String sql = "SELECT FileName,Directory FROM " + getFileSysTableName() + " WHERE DirId = ? AND FileName = ?;";

            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {

                pStmt.setInt(1, dirId);
                pStmt.setString(2, checkNameForSpecialChars(fname));

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] File exists SQL: " + pStmt);

                // Search for the file/folder
                try (ResultSet rs = pStmt.executeQuery()) {

                    // Check if a file record exists
                    if (rs.next()) {

                        // Check if the record is for a file or folder
                        if (rs.getBoolean("Directory"))
                            sts = FileStatus.DirectoryExists;
                        else
                            sts = FileStatus.FileExists;
                    }
                }
            }
        } catch (Exception ex) {
            // DEBUG
            logException("[H2] Error on fileExists: ", ex);
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
               // closeConnection(conn);
            }
        }

        // Return the status
        return sts;
    }

    /**
     * Create a file record for a new file or folder
     *
     * @param fname  String
     * @param dirId  int
     * @param params FileOpenParams
     * @param retain boolean
     * @return int
     * @throws DBException Database error
     * @throws FileExistsException File record already exists
     */
    public int createFileRecord(String fname, int dirId, FileOpenParams params, boolean retain)
            throws DBException, FileExistsException {

        // Create a new file record for a file/folder and return a unique file id
        Connection conn = null;

        int fileId = -1;
        boolean duplicateKey = false;

        try {

            // Get a database connection
            conn = getConnection();

            // Check if the file already exists in the database
            String chkFileName = checkNameForSpecialChars(fname);

            String qsql = "SELECT FileName,FileId FROM " + getFileSysTableName() + " WHERE FileName = ? AND DirId = ?";

            try (PreparedStatement pStmt = conn.prepareStatement(qsql)) {

                pStmt.setString(1, chkFileName);
                pStmt.setInt(2, dirId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Create file SQL: " + pStmt);

                // Check if the file/folder already exists
                try (ResultSet rs = pStmt.executeQuery()) {
                    if (rs.next()) {

                        // File record already exists, return the existing file id
                        fileId = rs.getInt("FileId");
                        Debug.println("File record already exists for " + fname + ", fileId=" + fileId);
                        return fileId;
                    }
                }
            }

            // Get a statement
            long timeNow = System.currentTimeMillis();
            String symLinkName = null;

            try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO "
                    + getFileSysTableName()
                    + "(FileName,CreateDate,ModifyDate,AccessDate,FileSize,DirId,Directory,ReadOnly,Archived,SystemFile,Hidden,Gid,Uid,Mode,IsSymLink,Encrypted)"
                    + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS)) {
                pstmt.setString(1, chkFileName);

                if ( params instanceof PopulateDBFileOpenParams) {
                    PopulateDBFileOpenParams dbParams = (PopulateDBFileOpenParams) params;

                    pstmt.setLong(2, dbParams.getCreationDateTime());
                    pstmt.setLong(3, dbParams.getModificationTimestamp());
                    pstmt.setLong(4, dbParams.getAccessTimestamp());

                    pstmt.setLong( 5, dbParams.getFileSize());

                    pstmt.setBoolean(16, dbParams.isEncrypted());

                    // Set the Unix fields
                    pstmt.setInt(12, dbParams.hasGid() ? dbParams.getGid() : 0);
                    pstmt.setInt(13, dbParams.hasUid() ? dbParams.getUid() : 0);
                    pstmt.setInt(14, dbParams.hasMode() ? dbParams.getMode() : 0);

                    if ( dbParams.isSymbolicLink())
                        symLinkName = dbParams.getSymbolicLinkName();
                }
                else {
                    pstmt.setLong(2, timeNow);         // CreateDate
                    pstmt.setLong(3, timeNow);         // ModifyDate
                    pstmt.setLong(4, timeNow);         // AccessDate

                    pstmt.setLong( 5, 0);           // FileSize

                    pstmt.setBoolean(16, false);    // Encrypted

                    // Set the Unix fields
                    if ( params instanceof UnixFileOpenParams) {
                        UnixFileOpenParams unixParams = (UnixFileOpenParams) params;

                        pstmt.setInt(12, unixParams.hasGid() ? unixParams.getGid() : 0);
                        pstmt.setInt(13, unixParams.hasUid() ? unixParams.getUid() : 0);
                        pstmt.setInt(14, unixParams.hasMode() ? unixParams.getMode() : 0);

                        if ( unixParams.isSymbolicLink())
                            symLinkName = unixParams.getSymbolicLinkName();
                    }
                    else {
                        pstmt.setInt( 12, 0);   // GID
                        pstmt.setInt( 13, 0);   // UID
                        pstmt.setInt( 14, 0);   // Mode
                    }
                }

                pstmt.setInt(6, dirId);
                pstmt.setBoolean(7, params.isDirectory());
                pstmt.setBoolean(8, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.ReadOnly));
                pstmt.setBoolean(9, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.Archive));
                pstmt.setBoolean(10, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.System));
                pstmt.setBoolean(11, FileAttribute.hasAttribute(params.getAttributes(), FileAttribute.Hidden));

                pstmt.setBoolean(15, params.isSymbolicLink());

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Create file SQL: " + pstmt);

                // Create an entry for the new file
                if (pstmt.executeUpdate() > 0) {

                    // Get the last insert id
                    try (ResultSet rs2 = pstmt.getGeneratedKeys()) {
                        if (rs2.next())
                            fileId = rs2.getInt(1);
                    }

                    // Check if the returned file id is valid
                    if (fileId == -1)
                        throw new DBException("Failed to get file id for " + fname);

                    // If retention is enabled then create a retention record for
                    // the new file/folder
                    if (retain && isRetentionEnabled()) {

                        // Create a retention record for the new file/directory
                        Timestamp startDate = new Timestamp(System.currentTimeMillis());
                        Timestamp endDate = new Timestamp(startDate.getTime() + getRetentionPeriod());

                        String rSql = "INSERT INTO " + getRetentionTableName() + " (FileId,StartDate,EndDate) VALUES (?,?,?);";

                        try (PreparedStatement pstmt2 = conn.prepareStatement(rSql)) {
                            pstmt2.setInt(1, fileId);
                            pstmt2.setTimestamp(2, startDate);
                            pstmt2.setTimestamp(3, endDate);

                            // DEBUG
                            if (Debug.EnableInfo && hasSQLDebug())
                                Debug.println("[H2] Add retention record SQL: " + pstmt2);

                            // Add the retention record for the file/folder
                            pstmt2.executeUpdate();
                        }
                    }

                    // Check if the new file is a symbolic link
                    if (params.isSymbolicLink() && symLinkName != null) {

                        // Create the symbolic link record
                        String symSql = "INSERT INTO " + getSymLinksTableName() + " (FileId, SymLink) VALUES (?,?);";

                        try (PreparedStatement pstmt3 = conn.prepareStatement(symSql)) {
                            pstmt3.setInt(1, fileId);
                            pstmt3.setString(2, symLinkName);

                            // DEBUG
                            if (Debug.EnableInfo && hasSQLDebug())
                                Debug.println("[H2] Create symbolic link SQL: " + pstmt3);

                            // Add the symbolic link record
                            pstmt3.executeUpdate();
                        }
                    }

                    // DEBUG
                    if (Debug.EnableInfo && hasDebug())
                        Debug.println("[H2] Created file name=" + fname + ", dirId=" + dirId + ", fileId=" + fileId);
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] Create file record SQLException: ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } catch (Exception ex) {

            // DEBUG
            logException("[H2] Create file record error ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
             //   closeConnection(conn);
            }
        }

        // If a duplicate key error occurred get the previously allocated file id
        if (duplicateKey) {

            // Get the previously allocated file id for the file record
            fileId = getFileId(dirId, fname, false, true);

            // DEBUG
            if (Debug.EnableInfo && hasSQLDebug())
                Debug.println("[H2] Duplicate key error, lookup file id, dirId=" + dirId + ", fname=" + fname + ", fid="
                        + fileId);
        }

        // Return the allocated file id
        return fileId;
    }

    /**
     * Create a stream record for a new file stream
     *
     * @param sname String
     * @param fid   int
     * @return int
     * @throws DBException Database error
     */
    public int createStreamRecord(String sname, int fid)
            throws DBException {

        // Make sure NTFS streams are enabled
        if (!isNTFSEnabled())
            throw new DBException("NTFS streams feature not enabled");

        // Create a new file stream attached to the specified file
        Connection conn = null;

        int streamId = -1;

        try {

            // Get a database connection
            conn = getConnection();

            // Get a statement
            long timeNow = System.currentTimeMillis();

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + getStreamsTableName()
                    + "(FileId,StreamName,CreateDate,ModifyDate,AccessDate,StreamSize) VALUES (?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS)) {
                stmt.setInt(1, fid);
                stmt.setString(2, sname);
                stmt.setLong(3, timeNow);
                stmt.setLong(4, timeNow);
                stmt.setLong(5, timeNow);
                stmt.setInt(6, 0);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Create stream SQL: " + stmt);

                // Create an entry for the new stream
                if (stmt.executeUpdate() > 0) {

                    // Get the stream id for the newly created stream
                    try (ResultSet rs2 = stmt.getGeneratedKeys()) {
                        if (rs2.next())
                            streamId = rs2.getInt(1);
                    }
                }
            }
        } catch (Exception ex) {

            // DEBUG
            logException("[H2] Create file stream error ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
              //  closeConnection(conn);
            }
        }

        // Return the allocated stream id
        return streamId;
    }

    /**
     * Delete a file or folder record
     *
     * @param dirId    int
     * @param fid      int
     * @param markOnly boolean
     * @throws DBException Database error
     */
    public void deleteFileRecord(int dirId, int fid, boolean markOnly)
            throws DBException {

        // Delete a file record from the database, or mark the file record as deleted
        Connection conn = null;
//        Statement stmt = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            String sql;

            if (markOnly)
                sql = "UPDATE " + getFileSysTableName() + " SET Deleted = 1 WHERE FileId = ?";
            else
                sql = "DELETE FROM " + getFileSysTableName() + " WHERE FileId = ?";

            // Delete the file entry from the database
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, fid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Delete file SQL: " + stmt);

                // Delete the file/folder, or mark as deleted
                int recCnt = stmt.executeUpdate();
                if (recCnt == 0) {
                    try (PreparedStatement pStmt =
                                 conn.prepareStatement("SELECT * FROM " + getFileSysTableName() + " WHERE FileId = ?")) {
                        pStmt.setInt(1, fid);

                        try (ResultSet rs = pStmt.executeQuery()) {
                            while (rs.next())
                                Debug.println("Found file " + rs.getString("FileName"));

                            throw new DBException("Failed to delete file record for fid=" + fid);
                        }
                    }
                }

                // Check if retention is enabled
                if (isRetentionEnabled()) {
                    // Delete the retention record for the file
                    sql = "DELETE FROM " + getRetentionTableName() + " WHERE FileId = ?";

                    try (PreparedStatement stmt2 = conn.prepareStatement(sql)) {
                        stmt2.setInt(1, fid);

                        // DEBUG
                        if (Debug.EnableInfo && hasSQLDebug())
                            Debug.println("[H2] Delete retention SQL: " + stmt2);

                        // Delete the file/folder retention record
                        stmt2.executeUpdate();
                    }
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] Delete file error ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            //    closeConnection(conn);
            }
        }
    }

    /**
     * Delete a file stream record
     *
     * @param fid      int
     * @param stid     int
     * @param markOnly boolean
     * @throws DBException Database error
     */
    public void deleteStreamRecord(int fid, int stid, boolean markOnly)
            throws DBException {

        // Make sure NTFS streams are enabled
        if (!isNTFSEnabled())
            return;

        // Delete a file stream from the database, or mark the stream as deleted
        Connection conn = null;

        try {
            // Get a database connection
            conn = getConnection();

            String sql = "DELETE FROM " + getStreamsTableName() + " WHERE FileId = ? AND StreamId = ?";

            // Get a statement
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, fid);
                stmt.setInt(2, stid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Delete stream SQL: " + stmt);

                // Delete the stream record
                stmt.executeUpdate();
            }
        } catch (Exception ex) {
            // DEBUG
            logException("[H2] Delete stream error: ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
         //       closeConnection(conn);
            }
        }
    }

    /**
     * Set file information for a file or folder
     *
     * @param dirId int
     * @param fid   int
     * @param finfo FileInfo
     * @throws DBException Database error
     */
    public void setFileInformation(int dirId, int fid, FileInfo finfo)
            throws DBException {

        // Set file information fields
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // To collect the param values to set in pStmt
            List<Object> params = new LinkedList<>();

            // Build the SQL statement to update the file information settings
            StringBuilder sql = new StringBuilder(256);
            sql.append("UPDATE ");
            sql.append(getFileSysTableName());
            sql.append(" SET ");

            // Check if the file attributes have been updated
            if (finfo.hasSetFlag(FileInfo.SetAttributes)) {

                // Update the basic file attributes
                sql.append("ReadOnly = ");
                sql.append(finfo.isReadOnly() ? "TRUE" : "FALSE");

                sql.append(", Archived =");
                sql.append(finfo.isArchived() ? "TRUE" : "FALSE");

                sql.append(", SystemFile = ");
                sql.append(finfo.isSystem() ? "TRUE" : "FALSE");

                sql.append(", Hidden = ");
                sql.append(finfo.isHidden() ? "TRUE" : "FALSE");
                sql.append(",");
            }

            // Check if the file size should be set
            if (finfo.hasSetFlag(FileInfo.SetFileSize)) {

                // Update the file size
                sql.append("FileSize = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getSize());
            }

            // Merge the group id, user id and mode into the in-memory file information
            if (finfo.hasSetFlag(FileInfo.SetGid)) {

                // Update the group id
                sql.append("Gid = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getGid());
            }

            if (finfo.hasSetFlag(FileInfo.SetUid)) {

                // Update the user id
                sql.append("Uid = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getUid());
            }

            if (finfo.hasSetFlag(FileInfo.SetMode)) {

                // Update the mode
                sql.append("Mode = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getMode());
            }

            // Check if the access date/time has been set
            if (finfo.hasSetFlag(FileInfo.SetAccessDate)) {

                // Add the SQL to update the access date/time
                sql.append(" AccessDate = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getAccessDateTime());
            }

            // Check if the modify date/time has been set
            if (finfo.hasSetFlag(FileInfo.SetModifyDate)) {

                // Add the SQL to update the modify date/time
                sql.append(" ModifyDate = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getModifyDateTime());
            }

            // Check if the inode change date/time has been set
            if (finfo.hasSetFlag(FileInfo.SetChangeDate)) {

                // Add the SQL to update the change date/time
                sql.append(" ChangeDate = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getChangeDateTime());
            }

            // Check if the creation date/time has been set
            if (finfo.hasSetFlag(FileInfo.SetCreationDate)) {

                // Add the SQL to update the creation date/time
                sql.append(" CreateDate = ");
                sql.append("?");
                sql.append(",");

                params.add(finfo.getCreationDateTime());
            }

            // Check if the file encryption status has been set
            if (finfo.hasSetFlag(FileInfo.SetEncrypted)) {

                // Add the SQL to update the creation date/time
                sql.append(" Encrypted = ");
                sql.append( finfo.isEncrypted() ? "TRUE" : "FALSE");
                sql.append(",");
            }

            // Trim any trailing comma
            if (sql.charAt(sql.length() - 1) == ',')
                sql.setLength(sql.length() - 1);

            // Complete the SQL request string
            sql.append(" WHERE FileId = ");
            sql.append("?");
            sql.append(";");

            params.add(fid);

            // Create the SQL statement

            try (PreparedStatement pStmt = conn.prepareStatement(sql.toString())) {
                setParams(pStmt, params);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Set file info SQL: " + pStmt);

                pStmt.executeUpdate();
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] SetFileInformation error: ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
         //       closeConnection(conn);
            }
        }
    }

    /**
     * Set information for a file stream
     *
     * @param dirId int
     * @param fid   int
     * @param stid  int
     * @param sinfo StreamInfo
     * @throws DBException Database error
     */
    public void setStreamInformation(int dirId, int fid, int stid, StreamInfo sinfo)
            throws DBException {

        // Set file stream information fields
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // To collect the param values to set in pStmt
            List<Object> params = new LinkedList<>();

            // Build the SQL statement to update the file information settings
            StringBuilder sql = new StringBuilder(256);
            sql.append("UPDATE ");
            sql.append(getStreamsTableName());
            sql.append(" SET ");

            // Check if the access date/time has been set
            if (sinfo.hasSetFlag(StreamInfo.SetAccessDate)) {

                // Add the SQL to update the access date/time
                sql.append(" AccessDate = ");
                sql.append("?");
                sql.append(",");

                params.add(sinfo.getAccessDateTime());
            }

            // Check if the modify date/time has been set
            if (sinfo.hasSetFlag(StreamInfo.SetModifyDate)) {

                // Add the SQL to update the modify date/time
                sql.append(" ModifyDate = ");
                sql.append("?");
                sql.append(",");

                params.add(sinfo.getModifyDateTime());
            }

            // Check if the stream size should be updated
            if (sinfo.hasSetFlag(StreamInfo.SetStreamSize)) {

                // Update the stream size
                sql.append(" StreamSize = ");
                sql.append("?");

                params.add(sinfo.getSize());
            }

            // Trim any trailing comma
            if (sql.charAt(sql.length() - 1) == ',')
                sql.setLength(sql.length() - 1);

            // Complete the SQL request string
            sql.append(" WHERE FileId = ");
            sql.append("?");
            sql.append(" AND StreamId = ");
            sql.append("?");
            sql.append(";");

            params.add(fid);
            params.add(stid);

            // Create the SQL statement
            try (PreparedStatement pStmt = conn.prepareStatement(sql.toString())) {
                setParams(pStmt, params);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Set stream info SQL: " + pStmt);

                pStmt.executeUpdate();
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] Set stream information error: ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            //    closeConnection(conn);
            }
        }
    }

    /**
     * Get the id for a file/folder, or -1 if the file/folder does not exist.
     *
     * @param dirId    int
     * @param fname    String
     * @param dirOnly  boolean
     * @param caseLess boolean
     * @return int
     * @throws DBException Database error
     */
    public int getFileId(int dirId, String fname, boolean dirOnly, boolean caseLess)
            throws DBException {

        // Get the file id for a file/folder
        int fileId = -1;

        // TODO: Remove this hardcoded false later
        // Hack alert: To improve the performance, we are changing the "JFileSrvFileSys" table -> "FileName" column
        // from VARCHAR to VARCHAR_IGNORECASE, So we are setting the caseLess = false always
        caseLess = false;

        Connection conn = null;

        try {

            // Get a connection to the database, create a statement for the database lookup
            conn = getConnection();

            // Build the SQL for the file lookup
            StringBuilder sql = new StringBuilder(128);

            sql.append("SELECT FileId FROM ");
            sql.append(getFileSysTableName());
            sql.append(" WHERE DirId = ");
            sql.append("?");
            sql.append(" AND ");

            // Check if the search is for a directory only
            if (dirOnly) {

                // Search for a directory record
                sql.append(" Directory = TRUE AND ");
            }

            // Check if the file name search should be caseless
            String fileName;
            if (caseLess) {

                // Perform a caseless search
                sql.append(" UPPER(FileName) = ");
                sql.append("?");

                fileName = checkNameForSpecialChars(fname).toUpperCase();
            } else {
                // TODO: This else logic is enough since the the FileName column type changed from VARCHAR to VARCHAR_IGNORECASE
                //  - Remove the caseLess check and if part logic later
                // Perform a case sensitive search
                sql.append(" FileName = ");
                sql.append("?");

                fileName = checkNameForSpecialChars(fname);
            }

            // Run the database search
            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

                stmt.setInt(1, dirId);
                stmt.setString(2, fileName);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Get file id SQL: " + stmt);

                try(ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        // Get the unique file id for the file or folder
                        fileId = rs.getInt("FileId");
                    }
                }
                // Check if a file record exists
            }
        } catch (Exception ex) {
            // DEBUG
            logException("[H2] Get file id error ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
              //  closeConnection(conn);
            }
        }

        // Return the file id, or -1 if not found
        return fileId;
    }

    /**
     * Get information for a file or folder
     *
     * @param dirId     int
     * @param fid       int
     * @param infoLevel FileInfoLevel
     * @return FileInfo
     * @throws DBException Database error
     */
    public DBFileInfo getFileInformation(int dirId, int fid, FileInfoLevel infoLevel)
            throws DBException {

        // Create a SQL select for the required file information
        StringBuilder sql = new StringBuilder(128);

        sql.append("SELECT ");

        // Select fields according to the required information level
        switch (infoLevel) {

            // File name only
            case NameOnly:
                sql.append("FileName");
                break;

            // File ids and name
            case Ids:
                sql.append("FileName,FileId,DirId");
                break;

            // All file information
            case All:
                sql.append("*");
                break;

            // Unknown information level
            default:
                throw new DBException("Invalid information level, " + infoLevel);
        }

        sql.append(" FROM ");
        sql.append(getFileSysTableName());
        sql.append(" WHERE FileId = ?");

        // Load the file record
        Connection conn = null;
        DBFileInfo finfo = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            try (PreparedStatement pStmt = conn.prepareStatement(sql.toString())) {
                pStmt.setInt(1, fid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Get file info SQL: " + pStmt);

                // Load the file record
                try (ResultSet rs = pStmt.executeQuery()) {
                    if (rs != null && rs.next()) {

                        // Create the file informaiton object
                        finfo = new DBFileInfo();
                        finfo.setFileId(fid);

                        // Load the file information
                        switch (infoLevel) {

                            // File name only
                            case NameOnly:
                                finfo.setFileName(rs.getString("FileName"));
                                break;

                            // File ids and name
                            case Ids:
                                finfo.setFileName(rs.getString("FileName"));
                                finfo.setDirectoryId(rs.getInt("DirId"));
                                break;

                            // All file information
                            case All:
                                finfo.setFileName(rs.getString("FileName"));
                                finfo.setSize(rs.getLong("FileSize"));
                                finfo.setAllocationSize(finfo.getSize());
                                finfo.setDirectoryId(rs.getInt("DirId"));

                                // Load the various file date/times
                                finfo.setCreationDateTime(rs.getLong("CreateDate"));
                                finfo.setModifyDateTime(rs.getLong("ModifyDate"));
                                finfo.setAccessDateTime(rs.getLong("AccessDate"));
                                finfo.setChangeDateTime(rs.getLong("ChangeDate"));

                                // Build the file attributes flags
                                int attr = 0;

                                if (rs.getBoolean("ReadOnly"))
                                    attr += FileAttribute.ReadOnly;

                                if (rs.getBoolean("SystemFile"))
                                    attr += FileAttribute.System;

                                if (rs.getBoolean("Hidden"))
                                    attr += FileAttribute.Hidden;

                                if (rs.getBoolean("Directory")) {
                                    attr += FileAttribute.Directory;
                                    finfo.setFileType(FileType.Directory);

                                    if ( finfo.getSize() == 0) {
                                        finfo.setAllocationSize( 512L);
                                        finfo.setSize( 512L);
                                    }
                                } else
                                    finfo.setFileType(FileType.RegularFile);

                                if (rs.getBoolean("Archived"))
                                    attr += FileAttribute.Archive;

                                finfo.setFileAttributes(attr);

                                // Get the group/owner id
                                finfo.setGid(rs.getInt("Gid"));
                                finfo.setUid(rs.getInt("Uid"));

                                finfo.setMode(rs.getInt("Mode"));

                                // Check if the file is a symbolic link
                                if (rs.getBoolean("IsSymLink"))
                                    finfo.setFileType(FileType.SymbolicLink);

                                // Check if the file is encrypted
                                finfo.setEncrypted(rs.getBoolean( "Encrypted"));
                                break;
                        }
                    }
                }

            }
        } catch (Exception ex) {
            // DEBUG
            logException("[H2] Get file information error ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
         //       closeConnection(conn);
            }
        }

        // Return the file information
        return finfo;
    }

    /**
     * Get information for a file stream
     *
     * @param fid       int
     * @param stid      int
     * @param infoLevel StreamInfoLevel
     * @return StreamInfo
     * @throws DBException Database error
     */
    public StreamInfo getStreamInformation(int fid, int stid, StreamInfoLevel infoLevel)
            throws DBException {

        // Make sure NTFS streams are enabled
        if (!isNTFSEnabled())
            return null;

        // Create a SQL select for the required stream information
        StringBuilder sql = new StringBuilder(128);

        sql.append("SELECT ");

        // Select fields according to the required information level
        switch (infoLevel) {

            // Stream name only.
            //
            // Also used if ids are requested as we already have the ids
            case NameOnly:
            case Ids:
                sql.append("StreamName");
                break;

            // All file information
            case All:
                sql.append("*");
                break;

            // Unknown information level
            default:
                throw new DBException("Invalid information level, " + infoLevel);
        }

        sql.append(" FROM ");
        sql.append(getStreamsTableName());
        sql.append(" WHERE FileId = ");
        sql.append("?");
        sql.append(" AND StreamId = ");
        sql.append("?");

        // Load the stream record
        Connection conn = null;

        StreamInfo sinfo = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            try (PreparedStatement pStmt = conn.prepareStatement(sql.toString())) {
                pStmt.setInt(1, fid);
                pStmt.setInt(2, stid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Get stream info SQL: " + pStmt);

                // Load the stream record
                try (ResultSet rs = pStmt.executeQuery()) {
                    if (rs != null && rs.next()) {

                        // Create the stream information object
                        sinfo = new StreamInfo("", fid, stid);

                        // Load the file information
                        switch (infoLevel) {

                            // Stream name only (or name and ids)
                            case NameOnly:
                            case Ids:
                                sinfo.setName(rs.getString("StreamName"));
                                break;

                            // All stream information
                            case All:
                                sinfo.setName(rs.getString("StreamName"));
                                sinfo.setSize(rs.getLong("StreamSize"));

                                // Load the various file date/times
                                sinfo.setCreationDateTime(rs.getLong("CreateDate"));
                                sinfo.setModifyDateTime(rs.getLong("ModifyDate"));
                                sinfo.setAccessDateTime(rs.getLong("AccessDate"));
                                break;
                        }
                    }
                }
            }
        } catch (Exception ex) {

            // DEBUG
            if (Debug.EnableError && hasDebug())
                Debug.println("[H2] Get stream information error " + ex.getMessage());

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null)
                releaseConnection(conn);
        }

        // Return the stream information
        return sinfo;
    }

    /**
     * Return the list of streams for the specified file
     *
     * @param fid       int
     * @param infoLevel StreamInfoLevel
     * @return StreamInfoList
     * @throws DBException Database error
     */
    public StreamInfoList getStreamsList(int fid, StreamInfoLevel infoLevel)
            throws DBException {

        // Make sure NTFS streams are enabled
        if (!isNTFSEnabled())
            return null;

        // Create a SQL select for the required stream information
        StringBuilder sql = new StringBuilder(128);

        sql.append("SELECT ");

        // Select fields according to the required information level
        switch (infoLevel) {

            // Stream name only.
            case NameOnly:
                sql.append("StreamName");
                break;

            // Stream name and ids
            case Ids:
                sql.append("StreamName,FileId,StreamId");
                break;

            // All file information
            case All:
                sql.append("*");
                break;

            // Unknown information level
            default:
                throw new DBException("Invalid information level, " + infoLevel);
        }

        sql.append(" FROM ");
        sql.append(getStreamsTableName());
        sql.append(" WHERE FileId = ");
        sql.append("?");

        // Load the stream record
        Connection conn = null;
        StreamInfoList sList = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            try (PreparedStatement pStmt = conn.prepareStatement(sql.toString())) {
                pStmt.setInt(1, fid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Get stream list SQL: " + pStmt);

                // Load the stream records
                try (ResultSet rs = pStmt.executeQuery()) {
                    sList = new StreamInfoList();

                    while (rs.next()) {

                        // Create the stream information object
                        StreamInfo sinfo = new StreamInfo("", fid, -1);

                        // Load the file information
                        switch (infoLevel) {

                            // Stream name only
                            case NameOnly:
                                sinfo.setName(rs.getString("StreamName"));
                                break;

                            // Stream name and id
                            case Ids:
                                sinfo.setName(rs.getString("StreamName"));
                                sinfo.setStreamId(rs.getInt("StreamId"));
                                break;

                            // All stream information
                            case All:
                                sinfo.setName(rs.getString("StreamName"));
                                sinfo.setStreamId(rs.getInt("StreamId"));
                                sinfo.setSize(rs.getLong("StreamSize"));

                                // Load the various file date/times
                                sinfo.setCreationDateTime(rs.getLong("CreateDate"));
                                sinfo.setModifyDateTime(rs.getLong("ModifyDate"));
                                sinfo.setAccessDateTime(rs.getLong("AccessDate"));
                                break;
                        }

                        // Add the stream information to the list
                        sList.addStream(sinfo);
                    }
                }
            }
        } catch (Exception ex) {

            // DEBUG
            if (Debug.EnableError && hasDebug())
                Debug.println("[H2] Get stream list error " + ex.getMessage());

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null)
                releaseConnection(conn);
        }

        // Return the streams list
        return sList;
    }

    /**
     * Rename a file or folder, may also change the parent directory.
     *
     * @param dirId   int
     * @param fid     int
     * @param newName String
     * @param newDir  int
     * @throws DBException Database error
     * @throws FileNotFoundException File record not found
     */
    public void renameFileRecord(int dirId, int fid, String newName, int newDir)
            throws DBException, FileNotFoundException {

        // Rename a file/folder
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Update the file record
            String sql = "UPDATE " + getFileSysTableName() + " SET FileName = ?, DirId = ?, ChangeDate = ? WHERE FileId = ?";

            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                pStmt.setString(1, checkNameForSpecialChars(newName));
                pStmt.setInt(2, newDir);
                pStmt.setLong(3, System.currentTimeMillis());
                pStmt.setInt(4, fid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Rename SQL: " + pStmt);

                // Rename the file/folder
                if (pStmt.executeUpdate() == 0) {

                    // Original file not found
                    throw new FileNotFoundException("" + fid);
                }
            }
        } catch (SQLException ex) {

            // DEBUG
            if (Debug.EnableError && hasDebug())
                Debug.println("[H2] Rename file error " + ex.getMessage());

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null)
                releaseConnection(conn);
        }
    }

    /**
     * Rename a file stream
     *
     * @param dirId   int
     * @param fid     int
     * @param stid    int
     * @param newName String
     * @throws DBException Database error
     */
    public void renameStreamRecord(int dirId, int fid, int stid, String newName)
            throws DBException {
        // TODO Auto-generated method stub

    }

    /**
     * Return the retention period expiry date/time for the specified file, or zero if the
     * file/folder is not under retention.
     *
     * @param dirId int
     * @param fid   int
     * @return RetentionDetails
     * @throws DBException Database error
     */
    public RetentionDetails getFileRetentionDetails(int dirId, int fid)
            throws DBException {

        // Check if retention is enabled
        if (!isRetentionEnabled())
            return null;

        // Get the retention record for the file/folder
        Connection conn = null;

        RetentionDetails retDetails = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Get the retention record, if any
            retDetails = getRetentionExpiryDateTime(conn, fid);
        } catch (SQLException ex) {

            // DEBUG
            if (Debug.EnableError && hasDebug())
                Debug.println("[H2] Get retention error " + ex.getMessage());

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null)
                releaseConnection(conn);
        }

        // Return the retention expiry date/time
        return retDetails;
    }

    /**
     * Start a directory search
     *
     * @param dirId      int
     * @param searchPath String
     * @param attrib     int
     * @param infoLevel  FileInfoLevel
     * @param maxRecords int
     * @return DBSearchContext
     * @throws DBException Database error
     */
    public DBSearchContext startSearch(int dirId, String searchPath, int attrib, FileInfoLevel infoLevel, int maxRecords)
            throws DBException {

        // To collect the param values to set in pStmt
        List<Object> params = new LinkedList<>();

        // Search for files/folders in the specified folder
        StringBuilder sql = new StringBuilder(128);
        sql.append("SELECT * FROM ");
        sql.append(getFileSysTableName());

        sql.append(" WHERE DirId = ");
        sql.append("?");
        sql.append(" AND Deleted = FALSE");

        params.add(dirId);

        // Split the search path
        String[] paths = FileName.splitPath(searchPath);

        // Check if the file name contains wildcard characters
        WildCard wildCard = null;

        if (WildCard.containsWildcards(searchPath)) {

            // For the '*.*' and '*' wildcards the SELECT will already return  all files/directories that are attached to the
            // parent directory. For 'name.*' and '*.ext' type wildcards we can use the LIKE clause to filter the required
            // records, for more complex wildcards we will post-process the search using the WildCard class to match the
            // file names.
            if (!searchPath.endsWith("\\*.*") && !searchPath.endsWith("\\*")) {

                // Create a wildcard search pattern
                wildCard = new WildCard(paths[1], true);

                // Check for a 'name.*' type wildcard
                if (wildCard.isType() == WildCard.Type.Ext) {

                    // Add the wildcard file extension selection clause to the SELECT
                    sql.append(" AND FileName LIKE(?)");

                    params.add(checkNameForSpecialChars(wildCard.getMatchPart()) + "%");

                    // Clear the wildcard object, we do not want it to filter the search results
                    wildCard = null;

                } else if (wildCard.isType() == WildCard.Type.Name) {

                    // Add the wildcard file name selection clause to the SELECT
                    sql.append(" AND FileName LIKE(?)");

                    params.add("%" + checkNameForSpecialChars(wildCard.getMatchPart()));

                    // Clear the wildcard object, we do not want it to filter the search results
                    wildCard = null;
                }
            }
        } else {

            // Search for a specific file/directory
            sql.append(" AND FileName = ?");

            params.add(checkNameForSpecialChars(paths[1]));
        }

        // Return directories first
        sql.append(" ORDER BY Directory DESC");

        // Start the search
        ResultSet rs;
        Connection conn = null;

        // Can't use this in try-with-resource since the resultSet is passed to next class for search -> H2SQLSearchContext(rs, wildCard);
        PreparedStatement pStmt = null;

        try {

            // Get a connection to the database
            conn = getConnection();
            pStmt = conn.prepareStatement(sql.toString());

            setParams(pStmt, params);

            // DEBUG
            if (Debug.EnableInfo && hasSQLDebug())
                Debug.println("[H2] Start search SQL: " + pStmt);

            // Start the folder search
            rs = pStmt.executeQuery();
        } catch (Exception ex) {
            // DEBUG
            logException("[H2] Start search error ", ex);

            // Rethrow the exception
            throw new DBException(ex.toString());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            }
            // Note: Don't close connection/resultSet, resultSet is passed to H2SQLSearchContext()
            // If we close then error --> The object is already closed [90007-212]
        }

        // Create the search context, and return
        return new H2SQLSearchContext(rs, wildCard);
    }

    /**
     * Return the used file space, or -1 if not supported.
     *
     * @return long
     */
    public long getUsedFileSpace() {

        // Calculate the total used file space
        Connection conn = null;

        long usedSpace = -1L;

        try {

            // Get a database connection and statement
            conn = getConnection();

            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT SUM(CAST(FileSize as BIGINT)) FROM " + getFileSysTableName();

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Get filespace SQL: " + sql);

                // Calculate the currently used disk space
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next())
                        usedSpace = rs.getLong(1);
                }
            }

        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] Get used file space error ", ex);

        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }

        // Return the used file space
        return usedSpace;
    }

    /**
     * Queue a file request.
     *
     * @param req FileRequest
     * @throws DBException Database error
     */
    public void queueFileRequest(FileRequest req)
            throws DBException {

        // Make sure the associated file state stays in memory for a short time,
        // if the queue is small the request may get processed soon.
        Statement stmt = null;

        if (req instanceof SingleFileRequest) {

            // Get the request details
            SingleFileRequest fileReq = (SingleFileRequest) req;

            try {

                // Check if the file request queue database connection is valid
                if (m_dbConn == null || m_dbConn.isClosed() || m_reqStmt == null || m_reqStmt.getConnection().isClosed())
                    createQueueStatements();

                // Check if the request is part of a transaction, or a standalone request
                if (!fileReq.isTransaction()) {

                    // Get a database connection
                    stmt = m_dbConn.createStatement();

                    // Write the file request record
                    int recCnt = 0;

                    synchronized (m_reqStmt) {

                        // Write the file request to the queue database
                        m_reqStmt.clearParameters();

                        m_reqStmt.setInt(1, fileReq.getFileId());
                        m_reqStmt.setInt(2, fileReq.getStreamId());
                        m_reqStmt.setInt(3, fileReq.isType().ordinal());
                        m_reqStmt.setString(4, fileReq.getTemporaryFile());
                        m_reqStmt.setString(5, fileReq.getVirtualPath());
                        m_reqStmt.setString(6, fileReq.getAttributesString());

                        if ( m_reqStmt.executeUpdate() > 0) {

                            // Get the last insert id
                            try (ResultSet rs2 = m_reqStmt.getGeneratedKeys()) {
                                if (rs2.next())
                                    fileReq.setSequenceNumber(rs2.getInt(1));
                            }
                        }
                    }
                } else {

                    // Check if the transaction prepared statement is valid, we
                    // may have lost the connection to the database.
                    if (m_tranStmt == null || m_tranStmt.getConnection().isClosed())
                        createQueueStatements();

                    // Write the transaction file request to the database
                    synchronized (m_tranStmt) {

                        // Write the request record to the database
                        m_tranStmt.clearParameters();

                        m_tranStmt.setInt(1, fileReq.getFileId());
                        m_tranStmt.setInt(2, fileReq.getStreamId());
                        m_tranStmt.setInt(3, fileReq.isType().ordinal());
                        m_tranStmt.setInt(4, fileReq.getTransactionId());
                        m_tranStmt.setString(5, fileReq.getTemporaryFile());
                        m_tranStmt.setString(6, fileReq.getVirtualPath());
                        m_tranStmt.setString(7, fileReq.getAttributesString());

                        m_tranStmt.executeUpdate();
                    }
                }

                // File request was queued successfully, check for any offline file requests
                if (hasOfflineFileRequests())
                    databaseOnlineStatus(DBStatus.Online);
            } catch (SQLException ex) {

                // If the request is a save then add to a pending queue to retry
                // when the database is back online
                if (fileReq.isType() == FileRequest.RequestType.Save || fileReq.isType() == FileRequest.RequestType.TransSave)
                    queueOfflineSaveRequest(fileReq);

                // DEBUG
                logException("[H2] queueFileRequest SQL Exception: ", ex);

                // Rethrow the exception
                throw new DBException(ex.getMessage());
            } finally {
                // Close the query statement
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        logException("[H2] queueFileRequest: Cannot close statement, ", ex);
                    }
                }
            }
        }
    }

    /**
     * Perform a queue cleanup deleting temporary cache files that do not have an associated save or
     * transaction request.
     *
     * @param tempDir        File
     * @param tempDirPrefix  String
     * @param tempFilePrefix String
     * @param jarFilePrefix  String
     * @return FileRequestQueue
     * @throws DBException Database error
     */
    public FileRequestQueue performQueueCleanup(File tempDir, String tempDirPrefix, String tempFilePrefix, String jarFilePrefix)
            throws DBException {

        // Get a connection to the database
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        FileRequestQueue reqQueue = new FileRequestQueue();

        try {

            // Get a connection to the database
            conn = getConnection(DBConnectionPool.PermanentLease);

            // Delete all load requests from the queue
            String fetchSql = "SELECT * FROM " + getQueueTableName() + " WHERE ReqType = ?;";

            try (PreparedStatement pStmt1 = conn.prepareStatement(fetchSql)) {
                pStmt1.setInt(1, FileRequest.RequestType.Load.ordinal());

                try (ResultSet fetchResultSet = pStmt1.executeQuery()) {
                    while (fetchResultSet.next()) {

                        // Get the path to the cache file
                        String tempPath = fetchResultSet.getString("TempFile");

                        // Check if the cache file exists, the file load may have been in progress
                        File tempFile = new File(tempPath);
                        if (tempFile.exists()) {

                            // Delete the cache file for the load request
                            tempFile.delete();

                            // DEBUG
                            if (Debug.EnableInfo && hasDebug())
                                Debug.println("[H2] Deleted load request file " + tempPath);
                        }
                    }
                }
            }

            // Check if the lock file exists, if so then the server did not shutdown cleanly
            File lockFile = new File(tempDir, LockFileName);
            setLockFile(lockFile.getAbsolutePath());

            boolean cleanShutdown = !lockFile.exists();

            // Create a crash recovery folder if the server did not shutdown clean
            File crashFolder = null;

            if (!cleanShutdown && hasCrashRecovery()) {

                // Create a unique crash recovery sub-folder in the temp area
                SimpleDateFormat dateFmt = new SimpleDateFormat("yyyyMMMdd_HHmmss");
                crashFolder = new File(tempDir, "CrashRecovery_" + dateFmt.format(new Date(System.currentTimeMillis())));
                if (crashFolder.mkdir()) {

                    // DEBUG
                    if (Debug.EnableDbg && hasDebug())
                        Debug.println("[H2] Created crash recovery folder - " + crashFolder.getAbsolutePath());
                } else {

                    // Use the top level temp area for the crash recovery files
                    crashFolder = tempDir;

                    // DEBUG
                    if (Debug.EnableDbg && hasDebug())
                        Debug.println("[H2] Failed to created crash recovery folder, using folder - "
                                + crashFolder.getAbsolutePath());
                }
            }

            // Delete the file load request records
            try (PreparedStatement pStmt2 = conn.prepareStatement("DELETE FROM " + getQueueTableName() + " WHERE ReqType = ?;")) {
                pStmt2.setInt(1, FileRequest.RequestType.Load.ordinal());

                pStmt2.execute();
            }

            // Create a statement to check if a temporary file is part of a save request
            pstmt = conn.prepareStatement("SELECT FileId,SeqNo FROM " + getQueueTableName() + " WHERE TempFile = ?;");

            // Scan all files/sub-directories within the temporary area looking for files that have
            // been saved but not deleted due to a server shutdown or crash.
            File[] tempFiles = tempDir.listFiles();

            if (tempFiles != null && tempFiles.length > 0) {

                // Scan the file loader sub-directories for temporary files
                for (File curFile : tempFiles) {

                    // Get the current file/sub-directory
                    if (curFile.isDirectory() && curFile.getName().startsWith(tempDirPrefix)) {

                        // Check if the sub-directory has any loader temporary files
                        File[] subFiles = curFile.listFiles();

                        if (subFiles != null && subFiles.length > 0) {

                            // Check each file to see if it has a pending save request in the file request database
                            for (int j = 0; j < subFiles.length; j++) {

                                // Get the current file from the list
                                File ldrFile = subFiles[j];

                                if (ldrFile.isFile() && ldrFile.getName().startsWith(tempFilePrefix)) {

                                    try {

                                        // Get the file details from the file system table
                                        pstmt.clearParameters();
                                        pstmt.setString(1, ldrFile.getAbsolutePath());

                                        rs = pstmt.executeQuery();

                                        if (rs.next()) {

                                            // File save request exists for temp file, nothing to do
                                        } else {

                                            // Check if the modified date indicates the file may have been updated
                                            if (ldrFile.lastModified() != 0L) {

                                                // Get the file id from the cache file name
                                                String fname = ldrFile.getName();
                                                int dotPos = fname.indexOf('.');
                                                String fidStr = fname.substring(tempFilePrefix.length(), dotPos);

                                                if (fidStr.indexOf('_') == -1) {

                                                    // Convert the file id
                                                    int fid = -1;

                                                    try {
                                                        fid = Integer.parseInt(fidStr);
                                                    } catch (NumberFormatException ex) {
                                                        logException("[H2]: perfomQueueCleanup NumberFormatException", ex);
                                                    }

                                                    // Get the file details from the database
                                                    if (fid != -1) {

                                                        // Get the file details for the temp file using the file id
                                                        try (PreparedStatement ps1 = conn.prepareStatement("SELECT * FROM " + getFileSysTableName()
                                                                + " WHERE FileId = ?;")) {
                                                            ps1.setInt(1, fid);

                                                            rs = ps1.executeQuery();
                                                        }

                                                        // If the previous server shutdown was clean then we may be able
                                                        // to queue the file save
                                                        if (cleanShutdown) {

                                                            if (rs.next()) {

                                                                // Get the currently stored modified date and file size for the associated file
                                                                long dbModDate = rs.getLong("ModifyDate");
                                                                long dbFileSize = rs.getLong("FileSize");

                                                                // Check if the temp file requires saving
                                                                if (ldrFile.length() != dbFileSize
                                                                        || ldrFile.lastModified() > dbModDate) {

                                                                    // Build the filesystem path to the file
                                                                    String filesysPath = buildPathForFileId(fid, conn);

                                                                    if (filesysPath != null) {

                                                                        // Create a file state for the file
                                                                        FileState fstate = m_dbCtx.getStateCache().findFileState(
                                                                                filesysPath, true);

                                                                        FileSegmentInfo fileSegInfo = (FileSegmentInfo) fstate
                                                                                .findAttribute(ObjectIdFileLoader.DBFileSegmentInfo);
                                                                        FileSegment fileSeg = null;

                                                                        if (fileSegInfo == null) {

                                                                            // Create a new file segment
                                                                            fileSegInfo = new FileSegmentInfo();
                                                                            fileSegInfo.setTemporaryFile(ldrFile
                                                                                    .getAbsolutePath());

                                                                            fileSeg = new FileSegment(fileSegInfo, true);
                                                                            fileSeg.setStatus(SegmentInfo.State.SaveWait, true);

                                                                            // Add the segment to the file state cache
                                                                            fstate.addAttribute(
                                                                                    ObjectIdFileLoader.DBFileSegmentInfo,
                                                                                    fileSegInfo);

                                                                            // Add a file save request for the temp file to the recovery queue
                                                                            reqQueue.addRequest(new SingleFileRequest(
                                                                                    FileRequest.RequestType.Save, fid, 0, ldrFile
                                                                                    .getAbsolutePath(), filesysPath,
                                                                                    fstate));

                                                                            // Update the file size and modified date/time in the filesystem database
                                                                            String updateSql = "UPDATE " + getFileSysTableName()
                                                                                    + " SET FileSize = ?, ModifyDate = ? WHERE FileId = ?;";

                                                                            try (PreparedStatement pStmtUpdate = conn.prepareStatement(updateSql)) {
                                                                                pStmtUpdate.setLong(1, ldrFile.length());
                                                                                pStmtUpdate.setLong(2, ldrFile.lastModified());
                                                                                pStmtUpdate.setInt(3, fid);

                                                                                pStmtUpdate.execute();
                                                                            }

                                                                            // DEBUG
                                                                            if (Debug.EnableInfo && hasDebug())
                                                                                Debug.println("[H2] Queued save request for "
                                                                                        + ldrFile.getName()
                                                                                        + ", path="
                                                                                        + filesysPath + ", fid=" + fid);
                                                                        }
                                                                    } else {

                                                                        // Delete the temp file, cannot resolve the path
                                                                        ldrFile.delete();

                                                                        // DEBUG
                                                                        if (Debug.EnableInfo && hasDebug())
                                                                            Debug.println("[H2] Cannot resolve filesystem path for FID "
                                                                                    + fid + ", deleted file " + ldrFile.getName());
                                                                    }
                                                                }
                                                            } else {

                                                                // Delete the temp file, file deos not exist in the filesystem table
                                                                ldrFile.delete();

                                                                // DEBUG
                                                                if (Debug.EnableInfo && hasDebug())
                                                                    Debug.println("[H2] No matching file record for FID "
                                                                            + fid + ", deleted file " + ldrFile.getName());
                                                            }
                                                        } else {

                                                            // File server did not shutdown cleanly so move any modified files to a holding area as they may be corrupt
                                                            if (rs.next() && hasCrashRecovery()) {

                                                                // Get the filesystem file name
                                                                String extName = rs.getString("FileName");

                                                                // Generate a file name to rename the cache file into a crash recovery folder
                                                                File crashFile = new File(crashFolder, "" + fid + "_" + extName);

                                                                // Rename the cache file into the crash recovery folder
                                                                if (ldrFile.renameTo(crashFile)) {

                                                                    // DEBUG
                                                                    if (Debug.EnableDbg && hasDebug())
                                                                        Debug.println("[H2] Crash recovery file - "
                                                                                + crashFile.getAbsolutePath());
                                                                }
                                                            } else {

                                                                // DEBUG
                                                                if (Debug.EnableDbg && hasDebug())
                                                                    Debug.println("[H2] Deleted incomplete cache file - "
                                                                            + ldrFile.getAbsolutePath());

                                                                // Delete the incomplte cache file
                                                                ldrFile.delete();
                                                            }
                                                        }
                                                    } else {

                                                        // Invalid file id format, delete the temp file
                                                        ldrFile.delete();

                                                        // DEBUG
                                                        if (Debug.EnableInfo && hasDebug())
                                                            Debug.println("[H2] Bad file id format, deleted file, "
                                                                    + ldrFile.getName());
                                                    }
                                                } else {

                                                    // Delete the temp file as it is for an NTFS tream
                                                    ldrFile.delete();

                                                    // DEBUG
                                                    if (Debug.EnableInfo && hasDebug())
                                                        Debug.println("[H2] Deleted NTFS stream temp file, "
                                                                + ldrFile.getName());
                                                }
                                            } else {

                                                // Delete the temp file as it has not been modified since it was loaded
                                                ldrFile.delete();

                                                // DEBUG
                                                if (Debug.EnableInfo && hasDebug())
                                                    Debug.println("[H2] Deleted unmodified temp file, "
                                                            + ldrFile.getName());
                                            }
                                        }
                                    } catch (SQLException ex) {
                                        Debug.println(ex);
                                    }
                                } else {

                                    // DEBUG
                                    if (Debug.EnableInfo && hasDebug())
                                        Debug.println("[H2] Deleted temporary file " + ldrFile.getName());

                                    // Delete the temporary file
                                    ldrFile.delete();
                                }
                            }
                        }
                    }
                }
            }

            // Create the lock file, delete any existing lock file
            if (lockFile.exists())
                lockFile.delete();

            try {
                lockFile.createNewFile();
            } catch (IOException ex) {

                // DEBUG
                if (Debug.EnableDbg && hasDebug())
                    Debug.println("[H2] Failed to create lock file - " + lockFile.getAbsolutePath());
            }
        } catch (SQLException ex) {

            // DEBUG
            if (Debug.EnableError && hasDebug())
                Debug.println(ex);
        } finally {
            // Close the prepared statement
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception ex) {
                    logException("[H2] performQueueCleanup: cannot close prepared statement", ex);
                }
            }

            // Close the result set
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ex) {
                    logException("[H2] performQueueCleanup: cannot close resultSet", ex);
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
             //   closeConnection(conn);
            }
        }

        // DEBUG
        if (Debug.EnableInfo && hasDebug())
            Debug.println("[H2] Cleanup recovered " + reqQueue.numberOfRequests() + " file saves from previous run");

        // Return the recovery file request queue
        return reqQueue;
    }

    /**
     * Check if the specified temporary file has a queued request.
     *
     * @param tempFile String
     * @param lastFile boolean
     * @return boolean
     * @throws DBException Database error
     */
    public boolean hasQueuedRequest(String tempFile, boolean lastFile)
            throws DBException {

        Connection conn = null;
        ResultSet rs = null;

        boolean queued = false;

        try {

            // Get a connection to the database
            conn = getConnection();

            String sql = "SELECT FileId FROM " + getQueueTableName() + " WHERE TempFile = ?;";

            try (PreparedStatement ps1 = conn.prepareStatement(sql)) {
                ps1.setString(1, tempFile);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Has queued req SQL: " + ps1);

                // Check if there is a queued request using the temporary file
                rs = ps1.executeQuery();
                if (rs.next())
                    queued = true;
                else {
                    // Check if there is a transaction using the temporary file
                    sql = "SELECT FileId FROM " + getTransactionTableName() + " WHERE TempFile = ?;";

                    try (PreparedStatement ps2 = conn.prepareStatement(sql)) {
                        ps2.setString(1, tempFile);

                        // DEBUG
                        if (Debug.EnableInfo && hasSQLDebug())
                            Debug.println("[H2] Has queued req SQL: " + ps2);

                        // Check the transaction table
                        rs = ps2.executeQuery();
                        if (rs.next())
                            queued = true;
                    }
                }
            }
        } catch (SQLException ex) {

            // DEBUG
            logException("[H2] hasQueueRequest: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Close the statement
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logException("[H2] hasQueueRequest: cannot close resultSet", ex);
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
           //     closeConnection(conn);
            }
        }

        // Return the queued status
        return queued;
    }

    /**
     * Delete a file request from the pending queue.
     *
     * @param fileReq FileRequest
     * @throws DBException Database error
     */
    public void deleteFileRequest(FileRequest fileReq)
            throws DBException {

        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Delete the file request queue entry from the request table or multiple records from the transaction table
            if (fileReq instanceof SingleFileRequest) {

                // Get the single file request details
                SingleFileRequest singleReq = (SingleFileRequest) fileReq;

                // Delete the request record
                String sql = "DELETE FROM " + getQueueTableName() + " WHERE SeqNo = ?";
                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setInt(1, singleReq.getSequenceNumber());

                    pStmt.executeUpdate();
                }
            } else {
                // Delete the transaction records
                String sql = "DELETE FROM " + getTransactionTableName() + " WHERE TranId = ?";
                try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                    pStmt.setInt(1, fileReq.getTransactionId());

                    pStmt.executeUpdate();
                }
            }
        } catch (SQLException ex) {

            // DEBUG
            logException("[H2] deleteFileRequest: SQLException: ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }
    }

    /**
     * Load a block of file request from the database into the specified queue.
     *
     * @param fromSeqNo int
     * @param reqType   FileRequest.RequestType
     * @param reqQueue  FileRequestQueue
     * @param recLimit  int
     * @return int
     * @throws DBException Database error
     */
    public int loadFileRequests(int fromSeqNo, FileRequest.RequestType reqType, FileRequestQueue reqQueue, int recLimit)
            throws DBException {

        // Load a block of file requests from the loader queue
        Connection conn = null;

        int recCnt = 0;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Build the SQL to load the queue records
            String sql = "SELECT * FROM " + getQueueTableName() + " WHERE SeqNo > ? AND ReqType = ? ORDER BY SeqNo LIMIT ?;";

            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                pStmt.setInt(1, fromSeqNo);
                pStmt.setInt(2, reqType.ordinal());
                pStmt.setInt(3, recLimit);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Load file requests - " + pStmt);

                // Get a block of file request records
                try (ResultSet rs = pStmt.executeQuery()) {
                    while (rs.next()) {

                        // Get the file request details
                        int fid = rs.getInt("FileId");
                        int stid = rs.getInt("StreamId");
                        FileRequest.RequestType reqTyp = FileRequest.RequestType.fromInt(rs.getInt("ReqType"));
                        int seqNo = rs.getInt("SeqNo");
                        String tempPath = rs.getString("TempFile");
                        String virtPath = rs.getString("VirtualPath");
                        String attribs = rs.getString("Attribs");

                        // Recreate the file request for the in-memory queue
                        SingleFileRequest fileReq = new SingleFileRequest(reqTyp, fid, stid, tempPath, virtPath, seqNo, null);
                        fileReq.setAttributes(attribs);

                        // Add the request to the callers queue
                        reqQueue.addRequest(fileReq);

                        // Update the count of loaded requests
                        recCnt++;
                    }
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] loadFileRequest: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            //    closeConnection(conn);
            }
        }

        // Return the count of file requests loaded
        return recCnt;
    }

    /**
     * Load a transaction request from the queue.
     *
     * @param tranReq MultiplFileRequest
     * @return MultipleFileRequest
     * @throws DBException Database error
     */
    public MultipleFileRequest loadTransactionRequest(MultipleFileRequest tranReq)
            throws DBException {

        // Load a transaction request from the transaction loader queue
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            String sql = "SELECT * FROM " + getTransactionTableName() + " WHERE TranId = ?;";

            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                pStmt.setInt(1, tranReq.getTransactionId());

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Load trans request - " + pStmt);

                // Get the block of file request records for the current transaction
                try (ResultSet rs = pStmt.executeQuery()) {

                    while (rs.next()) {

                        // Get the file request details
                        int fid = rs.getInt("FileId");
                        int stid = rs.getInt("StreamId");
                        String tempPath = rs.getString("TempFile");
                        String virtPath = rs.getString("VirtualPath");

                        // Create the cached file information and add to the request
                        CachedFileInfo finfo = new CachedFileInfo(fid, stid, tempPath, virtPath);
                        tranReq.addFileInfo(finfo);
                    }
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] loadTransactionRequest SQLException: ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            //    closeConnection(conn);
            }
        }

        // Return the updated file request
        return tranReq;
    }

    /**
     * Shutdown the database interface, release resources.
     *
     * @param context DBDeviceContext
     */
    public void shutdownDatabase(DBDeviceContext context) {

        // Call the base class
        super.shutdownDatabase(context);
    }

    /**
     * Get the retention expiry date/time for a file/folder
     *
     * @param conn Connection
     * @param fid  int
     * @return RetentionDetails
     * @throws SQLException SQL error
     */
    private final RetentionDetails getRetentionExpiryDateTime(Connection conn, int fid)
            throws SQLException {

        // Get the retention expiry date/time for the specified file/folder
        RetentionDetails retDetails = null;
        String sql = "SELECT StartDate,EndDate FROM " + getRetentionTableName() + " WHERE FileId = ?;";

        try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
            pStmt.setInt(1, fid);

            // DEBUG
            if (Debug.EnableInfo && hasSQLDebug())
                Debug.println("[H2] Get retention expiry SQL: " + pStmt);

            // Get the retention record, if any
            try (ResultSet rs = pStmt.executeQuery()) {
                if (rs.next()) {

                    // Get the retention expiry date
                    Timestamp startDate = rs.getTimestamp("StartDate");
                    Timestamp endDate = rs.getTimestamp("EndDate");

                    retDetails = new RetentionDetails(fid, startDate != null ? startDate.getTime() : -1L, endDate.getTime());
                }
            }
        }

        // Return the retention expiry date/time
        return retDetails;
    }

    /**
     * Determine if the specified file/folder is still within an active retention period
     *
     * @param conn Connection
     * @param stmt Statement
     * @param fid  int
     * @return boolean
     * @throws SQLException SQL error
     */
    private final boolean fileHasActiveRetention(Connection conn, Statement stmt, int fid)
            throws SQLException {

        // Check if retention is enabled
        if (!isRetentionEnabled())
            return false;

        // Check if the file/folder is within the retention period
        RetentionDetails retDetails = getRetentionExpiryDateTime(conn, fid);
        if (retDetails == null)
            return false;

        // File/folder is within the retention period
        return retDetails.isWithinRetentionPeriod(System.currentTimeMillis());
    }

    /**
     * Create the prepared statements used by the file request queueing database
     *
     * @throws SQLException SQL error
     */
    protected final void createQueueStatements()
            throws SQLException {

        // Check if the database connection is valid
        if (m_dbConn != null) {

            // Close the existing statements
            if (m_reqStmt != null)
                m_reqStmt.close();

            if (m_tranStmt != null)
                m_tranStmt.close();

            // Release the current database connection
            releaseConnection(m_dbConn);
            m_dbConn = null;

        }

        if (m_dbConn == null)
            m_dbConn = getConnection(DBConnectionPool.PermanentLease);

        // Create the prepared statements for accessing the file request queue database
        m_reqStmt = m_dbConn.prepareStatement("INSERT INTO " + getQueueTableName()
                + "(FileId,StreamId,ReqType,TempFile,VirtualPath,Attribs) VALUES (?,?,?,?,?,?);");

        // Create the prepared statements for accessing the transaction request queue database
        m_tranStmt = m_dbConn.prepareStatement("INSERT INTO " + getTransactionTableName()
                + "(FileId,StreamId,ReqType,TranId,TempFile,VirtualPath,Attribs) VALUES (?,?,?,?,?,?,?);");
    }

    /**
     * Return the file data details for the specified file or stream.
     *
     * @param fileId   int
     * @param streamId int
     * @return DBDataDetails
     * @throws DBException Database error
     */
    public DBDataDetails getFileDataDetails(int fileId, int streamId)
            throws DBException {

        // Load the file details from the data table
        Connection conn = null;
        DBDataDetails dbDetails = null;

        try {
            // Get a connection to the database
            conn = getConnection();

            String sql = "SELECT * FROM " + getDataTableName() + " WHERE FileId = ? AND StreamId = ? AND FragNo = 1;";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, fileId);
                stmt.setInt(2, streamId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Get file data details SQL: " + stmt);

                // Load the file details
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {

                        // Create the file details
                        dbDetails = new DBDataDetails(fileId, streamId);

                        if (rs.getBoolean("JarFile"))
                            dbDetails.setJarId(rs.getInt("JarId"));
                    }
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] getFileDataDetails: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }

        // If the file details are not valid throw an exception
        if (dbDetails == null)
            throw new DBException("Failed to load file details for " + fileId + ":" + streamId);

        // Return the file data details
        return dbDetails;
    }

    /**
     * Return the maximum data fragment size supported
     *
     * @return long
     */
    public long getMaximumFragmentSize() {
        return 20 * MemorySize.MEGABYTE;
    }

    /**
     * Load file data from the database into a temporary/local file
     *
     * @param fileId   int
     * @param streamId int
     * @param fileSeg  FileSegment
     * @throws DBException Database error
     * @throws IOException I/O error
     */
    public void loadFileData(int fileId, int streamId, FileSegment fileSeg)
            throws DBException, IOException {

        // Open the temporary file
        FileOutputStream fileOut = new FileOutputStream(fileSeg.getTemporaryFile());

        // Update the segment status
        fileSeg.setStatus(FileSegmentInfo.State.Loading);

        // DEBUG
        long startTime = 0L;

        if (Debug.EnableInfo && hasDebug())
            startTime = System.currentTimeMillis();

        // Load the file data fragments
        Connection conn = null;

        try {

            // Get a connection to the database, create a statement
            conn = getConnection();

            String sql = "SELECT * FROM " + getDataTableName() + " WHERE FileId = ? AND StreamId = ? ORDER BY FragNo";

            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                pStmt.setInt(1, fileId);
                pStmt.setInt(2, streamId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[mySQL] Load file data SQL: " + pStmt);

                // Find the data fragments for the file, check if the file is stored in a Jar
                try (ResultSet rs = pStmt.executeQuery()) {
                    // Load the file data from the main file record(s)
                    byte[] inbuf = null;
                    int buflen = 0;
                    int fragNo = -1;
                    int fragSize = -1;

                    long totLen = 0L;

                    while (rs.next()) {

                        // Access the file data
                        Blob dataBlob = rs.getBlob("Data");
                        fragNo = rs.getInt("FragNo");
                        fragSize = rs.getInt("FragLen");

                        InputStream dataFrag = dataBlob.getBinaryStream();

                        // Allocate the read buffer, if not already allocated
                        if (inbuf == null) {
                            buflen = (int) Math.min(dataBlob.length(), MaxMemoryBuffer);
                            inbuf = new byte[buflen];
                        }

                        // Read the data from the database record and write to the output file
                        int rdLen = dataFrag.read(inbuf, 0, inbuf.length);

                        while (rdLen > 0) {

                            // Write a block of data to the temporary file segment
                            fileOut.write(inbuf, 0, rdLen);
                            totLen += rdLen;

                            // Read another block of data
                            rdLen = dataFrag.read(inbuf, 0, inbuf.length);
                        }

                        // Signal to waiting threads that data is available
                        fileSeg.setReadableLength(totLen);
                        fileSeg.signalDataAvailable();

                        // Renew the lease on the database connection
                        getConnectionPool().renewLease(conn);
                    }

                    // DEBUG
                    if (Debug.EnableInfo && hasDebug()) {
                        long endTime = System.currentTimeMillis();
                        Debug.println("[mySQL] Loaded fid=" + fileId + ", stream=" + streamId + ", frags=" + fragNo + ", time="
                                + (endTime - startTime) + "ms");
                    }
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] loadFileDate: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
            //    closeConnection(conn);
            }

            // Close the output file
            if (fileOut != null) {
                try {
                    fileOut.close();
                } catch (Exception ex) {
                    logException("[H2] loadFileDate: cannot close output file, ", ex);
                }
            }
        }

        // Signal that the file data is available
        fileSeg.signalDataAvailable();
    }

    /**
     * Load Jar file data from the database into a temporary file
     *
     * @param jarId  int
     * @param jarSeg FileSegment
     * @throws DBException Database error
     * @throws IOException I/O error
     */
    public void loadJarData(int jarId, FileSegment jarSeg)
            throws DBException, IOException {

        // Load the Jar file data
        Connection conn = null;

        FileOutputStream outJar = null;

        try {

            // Get a connection to the database, create a statement
            conn = getConnection();

            String sql = "SELECT * FROM " + getJarDataTableName() + " WHERE JarId = ?";

            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                pStmt.setInt(1, jarId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[mySQL] Load Jar data SQL: " + pStmt);

                // Create the temporary Jar file
                outJar = new FileOutputStream(jarSeg.getTemporaryFile());

                // Get the Jar data record
                try (ResultSet rs = pStmt.executeQuery()) {
                    if (rs.next()) {

                        // Access the Jar file data
                        Blob dataBlob = rs.getBlob("Data");
                        InputStream dataFrag = dataBlob.getBinaryStream();

                        // Allocate the read buffer
                        byte[] inbuf = new byte[(int) Math.min(dataBlob.length(), MaxMemoryBuffer)];

                        // Read the Jar data from the database record and write to the output file
                        int rdLen = dataFrag.read(inbuf, 0, inbuf.length);
                        long totLen = 0L;

                        while (rdLen > 0) {

                            // Write a block of data to the temporary file segment
                            outJar.write(inbuf, 0, rdLen);
                            totLen += rdLen;

                            // Read another block of data
                            rdLen = dataFrag.read(inbuf, 0, inbuf.length);
                        }
                    }

                    // Close the output Jar file
                    outJar.close();

                    // Set the Jar file segment status to indicate that the data has been loaded
                    jarSeg.setStatus(FileSegmentInfo.State.Available, false);
                }
            }

        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] loadJarData: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
         //       closeConnection(conn);
            }

            // Close the output file
            if (outJar != null) {
                try {
                    outJar.close();
                } catch (Exception ex) {
                    logException("[H2] loadJarData: error while closing output file, ", ex);
                }
            }
        }
    }

    /**
     * Save the file data from the temporary/local file to the database
     *
     * @param fileId   int
     * @param streamId int
     * @param fileSeg  FileSegment
     * @return int
     * @throws DBException Database error
     * @throws IOException I/O error
     */
    public int saveFileData(int fileId, int streamId, FileSegment fileSeg)
            throws DBException, IOException {

        // Determine if we can use an in memory buffer to copy the file fragments
        boolean useMem = false;
        byte[] memBuf = null;

        if (getDataFragmentSize() <= MaxMemoryBuffer) {

            // Use a memory buffer to copy the file data fragments
            useMem = true;
            memBuf = new byte[(int) getDataFragmentSize()];
        }

        // Get the temporary file size
        File tempFile = new File(fileSeg.getTemporaryFile());

        // Save the file data
        Connection conn = null;
        PreparedStatement stmt = null;
        int fragNo = 1;

        FileInputStream inFile = null;

        try {

            // Open the temporary file
            inFile = new FileInputStream(tempFile);

            // Get a connection to the database
            conn = getConnection();

            String sql = "DELETE FROM " + getDataTableName() + " WHERE FileId = ? AND StreamId = ?";

            try (PreparedStatement delStmt = conn.prepareStatement(sql)) {
                delStmt.setInt(1, fileId);
                delStmt.setInt(2, streamId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[mySQL] Save file data SQL: " + delStmt);

                // Delete any existing file data records for this file
                int recCnt = delStmt.executeUpdate();
            }

            // Add the file data to the database
            stmt = conn.prepareStatement("INSERT INTO " + getDataTableName()
                    + " (FileId,StreamId,FragNo,FragLen,Data) VALUES (?,?,?,?,?)");

            // DEBUG
            if (Debug.EnableInfo && hasSQLDebug())
                Debug.println("[mySQL] Save file data SQL: " + stmt.toString());

            long saveSize = tempFile.length();

            while (saveSize > 0) {

                // Determine the current fragment size to store
                long fragSize = Math.min(saveSize, getDataFragmentSize());

                // Determine if the data fragment should be copied to a memory buffer or a seperate
                // temporary file
                InputStream fragStream = null;

                if (saveSize == fragSize) {

                    // Just copy the data from the temporary file, only one fragment
                    fragStream = inFile;
                } else if (useMem) {

                    // Copy a block of data to the memory buffer
                    fragSize = inFile.read(memBuf);
                    fragStream = new ByteArrayInputStream(memBuf);
                } else {

                    // Need to create a temporary file and copy the fragment of data to it
                    throw new DBException("File data copy not implemented yet");
                }

                // Store the current fragment
                stmt.clearParameters();
                stmt.setInt(1, fileId);
                stmt.setInt(2, streamId);
                stmt.setInt(3, fragNo++);
                stmt.setInt(4, (int) fragSize);
                stmt.setBinaryStream(5, fragStream, (int) fragSize);

                if (stmt.executeUpdate() < 1 && hasDebug())
                    Debug.println("## mySQL Failed to update file data, fid=" + fileId + ", stream=" + streamId + ", fragNo="
                            + (fragNo - 1));

                // Update the remaining data size to be saved
                saveSize -= fragSize;

                // Renew the lease on the database connection so that it does not expire
                getConnectionPool().renewLease(conn);
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] saveFileData: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Close the insert statement
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                    logException("[H2] saveFileData: cannot close statement, ", ex);
                }
            }

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
           //     closeConnection(conn);
            }

            // Close the input file
            if (inFile != null) {
                try {
                    inFile.close();
                } catch (Exception ex) {
                    logException("[H2] saveFileData: cannot close input file, ", ex);
                }
            }
        }

        // Return the number of data fragments used to save the file data
        return fragNo;
    }

    /**
     * Save the file data from a Jar file to the database
     *
     * @param jarPath  String
     * @param fileList DBDataDetailsList
     * @return int
     * @throws DBException Database error
     * @throws IOException I/O error
     */
    public int saveJarData(String jarPath, DBDataDetailsList fileList)
            throws DBException, IOException {

        // Write the Jar file to the blob field in the Jar data table
        Connection conn = null;

        int jarId = -1;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Open the Jar file
            File jarFile = new File(jarPath);
            FileInputStream inJar = new FileInputStream(jarFile);

            // Add the Jar file data to the database
            try (PreparedStatement istmt = conn.prepareStatement("INSERT INTO " + getJarDataTableName() + " (Data) VALUES (?)")) {
                // Set the Jar data field
                istmt.setBinaryStream(1, inJar, (int) jarFile.length());

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[mySQL] Save Jar data SQL: " + istmt);

                if (istmt.executeUpdate() < 1 && hasDebug())
                    Debug.println("## mySQL Failed to store Jar data");
            }

            // Get the unique jar id allocated to the new Jar record
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT LAST_INSERT_ID();")
            ) {
                if (rs.next())
                    jarId = rs.getInt(1);
            }

            // Update the jar id record for each file in the Jar
            for (int i = 0; i < fileList.numberOfFiles(); i++) {

                // Get the current file details
                DBDataDetails dbDetails = fileList.getFileAt(i);

                String delQuery = "DELETE FROM " + getDataTableName() + " WHERE FileId = ? AND StreamId = ?";

                try (PreparedStatement pStmt = conn.prepareStatement(delQuery)) {
                    pStmt.setInt(1, dbDetails.getFileId());
                    pStmt.setInt(2, dbDetails.getStreamId());

                    pStmt.executeUpdate();
                }

                // Add the file data record(s) to the database
                String insertQuery = "INSERT INTO " + getDataTableName() + " (FileId,StreamId,FragNo,JarId,JarFile) VALUES (?,?,?,?,?);";

                try (PreparedStatement pStmt2 = conn.prepareStatement(insertQuery)) {
                    pStmt2.setInt(1, dbDetails.getFileId());
                    pStmt2.setInt(2, dbDetails.getStreamId());
                    pStmt2.setInt(3, 1);
                    pStmt2.setInt(4, jarId);
                    pStmt2.setInt(5, 1);

                    pStmt2.executeUpdate();
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] saveJarData: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }

        // Return the allocated Jar id
        return jarId;
    }

    /**
     * Delete the file data for the specified file/stream
     *
     * @param fileId   int
     * @param streamId int
     * @throws DBException Database error
     * @throws IOException I/O error
     */
    public void deleteFileData(int fileId, int streamId)
            throws DBException, IOException {

        // Delete the file data records for the file or stream
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Need to delete the existing data
            String sql;

            // Check if the main file stream is being deleted, if so then delete all stream data too
            if (streamId == 0)
                sql = "DELETE FROM " + getDataTableName() + " WHERE FileId = ?";
            else
                sql = "DELETE FROM " + getDataTableName() + " WHERE FileId = ? AND StreamId = ?";

            try (PreparedStatement delStmt = conn.prepareStatement(sql)) {
                delStmt.setInt(1, fileId);

                if (streamId != 0) {
                    delStmt.setInt(2, streamId);
                }

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[mySQL] Delete file data SQL: " + delStmt);

                // Delete the file data records
                int recCnt = delStmt.executeUpdate();

                // Debug
                if (Debug.EnableInfo && hasDebug() && recCnt > 0)
                    Debug.println("[mySQL] Deleted file data fid=" + fileId + ", stream=" + streamId + ", records=" + recCnt);
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] deleteFileData: SQLException, ", ex);
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
           //     closeConnection(conn);
            }
        }
    }

    /**
     * Delete the file data for the specified Jar file
     *
     * @param jarId int
     * @throws DBException Database error
     * @throws IOException I/O error
     */
    public void deleteJarData(int jarId)
            throws DBException, IOException {

        // Delete the data records for the Jar file data
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Need to delete the existing data
            String sql = "DELETE FROM " + getJarDataTableName() + " WHERE JarId = ?;";
            try (PreparedStatement delStmt = conn.prepareStatement(sql)) {
                delStmt.setInt(1, jarId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[mySQL] Delete Jar data SQL: " + delStmt);

                // Delete the Jar data records
                int recCnt = delStmt.executeUpdate();

                // Debug
                if (Debug.EnableInfo && hasDebug() && recCnt > 0)
                    Debug.println("[mySQL] Deleted Jar data jarId=" + jarId + ", records=" + recCnt);
            }
        } catch (SQLException ex) {

            // DEBUG
            logException("[H2] deleteJarData: SQLException, ", ex);
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }
    }

    // ***** DBObjectIdInterface Methods *****

    /**
     * Create a file id to object id mapping
     *
     * @param fileId   int
     * @param streamId int
     * @param objectId String
     * @throws DBException Database error
     */
    public void saveObjectId(int fileId, int streamId, String objectId)
            throws DBException {

        // Create a new file id/object id mapping record
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            // Delete any current mapping record for the object
            String sql = "DELETE FROM " + getObjectIdTableName() + " WHERE FileId = ? AND StreamId = ?";

            try (PreparedStatement pStmt = conn.prepareStatement(sql)) {
                pStmt.setInt(1, fileId);
                pStmt.setInt(2, streamId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Save object id SQL: " + pStmt);

                // Delete any current mapping record
                pStmt.executeUpdate();
            }

            // Insert the new mapping record
            sql = "INSERT INTO " + getObjectIdTableName() + " (FileId,StreamId,ObjectID) VALUES(?,?,?)";

            try (PreparedStatement pStmt2 = conn.prepareStatement(sql)) {
                pStmt2.setInt(1, fileId);
                pStmt2.setInt(2, streamId);
                pStmt2.setString(3, objectId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Save object id SQL: " + pStmt2);

                // Create the mapping record
                if (pStmt2.executeUpdate() == 0)
                    throw new DBException("Failed to add object id record, fid=" + fileId + ", objId=" + objectId);
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] saveObjectId: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
         //       closeConnection(conn);
            }
        }
    }

    /**
     * Load the object id for the specified file id
     *
     * @param fileId   int
     * @param streamId int
     * @return String
     * @throws DBException Database error
     */
    public String loadObjectId(int fileId, int streamId)
            throws DBException {

        // Load the object id for the specified file id
        Connection conn = null;
        String objectId = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            String sql = "SELECT ObjectId FROM " + getObjectIdTableName() + " WHERE FileId = ? AND StreamId = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, fileId);
                stmt.setInt(2, streamId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Load object id SQL: " + stmt);

                // Load the mapping record
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next())
                        objectId = rs.getString("ObjectId");
                }
            }
        } catch (SQLException ex) {

            // DEBUG
            logException("[H2] loadObjectId: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }

        // Return the object id
        return objectId;
    }

    /**
     * Delete a file id/object id mapping
     *
     * @param fileId   int
     * @param streamId int
     * @param objectId String
     * @throws DBException Database error
     */
    public void deleteObjectId(int fileId, int streamId, String objectId)
            throws DBException {

        // Delete a file id/object id mapping record
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            String sql = "DELETE FROM " + getObjectIdTableName() + " WHERE FileId = ? AND StreamId = ?";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setInt(1, fileId);
                stmt.setInt(2, streamId);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Delete object id SQL: " + stmt);

                // Delete the mapping record
                stmt.executeUpdate();
            }
        } catch (SQLException ex) {

            // DEBUG
            logException("[H2] deleteObjectId: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {

            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }
    }

    /**
     * Return the data for a symbolic link
     *
     * @param dirId int
     * @param fid   int
     * @return String
     * @throws DBException Database error
     */
    public String readSymbolicLink(int dirId, int fid)
            throws DBException {

        // Delete a file id/object id mapping record
        Connection conn = null;

        String symLink = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            String sql = "SELECT SymLink FROM " + getSymLinksTableName() + " WHERE FileId = ?";

            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setInt(1, fid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Read symbolic link: " + statement);

                // Load the mapping record
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next())
                        symLink = rs.getString("SymLink");
                    else
                        throw new DBException("Failed to load symbolic link data for " + fid);
                }
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] readSymbolicLink: SQLException, ", ex);

            // Rethrow the exception
            throw new DBException(ex.getMessage());
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
          //      closeConnection(conn);
            }
        }

        // Return the symbolic link data
        return symLink;
    }

    /**
     * Delete a symbolic link record
     *
     * @param dirId int
     * @param fid   int
     * @throws DBException Database error
     */
    public void deleteSymbolicLinkRecord(int dirId, int fid)
            throws DBException {

        // Delete the symbolic link record for a file
        Connection conn = null;

        try {

            // Get a connection to the database
            conn = getConnection();

            String sql = "DELETE FROM " + getSymLinksTableName() + " WHERE FileId = ?";

            try (PreparedStatement delStmt = conn.prepareStatement(sql)) {
                delStmt.setInt(1, fid);

                // DEBUG
                if (Debug.EnableInfo && hasSQLDebug())
                    Debug.println("[H2] Delete symbolic link SQL: " + delStmt);

                // Delete the symbolic link record
                int recCnt = delStmt.executeUpdate();

                // Debug
                if (Debug.EnableInfo && hasDebug() && recCnt > 0)
                    Debug.println("[H2] Deleted symbolic link fid=" + fid);
            }
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] deleteSymbolicLinkRecord: SQLException, ", ex);
        } finally {
            // Release the database connection
            if (conn != null) {
                releaseConnection(conn);
         //       closeConnection(conn);
            }
        }
    }

    /**
     * Convert a file id to a share relative path
     *
     * @param fileid int
     * @param conn Connection
     * @return String
     */
    private String buildPathForFileId(int fileid, Connection conn) {

        // Build an array of folder names working back from the files id
        StringList names = new StringList();

        try (PreparedStatement pStmt = conn.prepareStatement(
                "SELECT DirId,FileName FROM " + getFileSysTableName() + " WHERE FileId = ?;")) {

            // Loop, walking backwards up the tree until we hit root
            int curFid = fileid;

            do {
                pStmt.setInt(1, curFid);

                // Search for the current file record in the database
                try (ResultSet rs = pStmt.executeQuery()) {

                    if (rs.next()) {

                        // Get the filename
                        names.addString(rs.getString("FileName"));

                        // The directory id becomes the next file id to search for
                        curFid = rs.getInt("DirId");
                    } else
                        return null;

                }
            } while (curFid > 0);
        } catch (SQLException ex) {
            // DEBUG
            logException("[H2] buildPathFileId: SQLException, ", ex);

            return null;
        }

        // Build the path string
        StringBuilder pathStr = new StringBuilder(256);
        pathStr.append(FileName.DOS_SEPERATOR_STR);

        for (int i = names.numberOfStrings() - 1; i >= 0; i--) {
            pathStr.append(names.getStringAt(i));
            pathStr.append(FileName.DOS_SEPERATOR_STR);
        }

        // Remove the trailing slash from the path
        if (pathStr.length() > 0)
            pathStr.setLength(pathStr.length() - 1);

        // Return the path string
        return pathStr.toString();
    }

    /**
     * Log the message and full stack trace for debugging purposes
     *
     * @param message String
     * @param ex Exception
     */
    private void logException(String message, Exception ex) {

        // Print message about method name and useful info
        if ( hasDebug())
            Debug.println(message);

        // Print Full stack trace
        if ( Debug.hasDumpStackTraces())
            Debug.println(ex);
    }

    private void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
                logException("[H2] Error while closing connection: ", ex);
            }
        }
    }

    /**
     * To set the collected params values to PreparedStatement based on
     * index, type in case of dynamic condition queries
     * @param ps PreparedStatement
     * @param params List&lt;Object&gt;
     * @throws SQLException Database error
     */
    public void setParams(PreparedStatement ps, List<Object> params) throws SQLException {

        int idx = 1;

        // Iterate params value and set in preparedStatement based on type
        for (Object value : params) {

            if (value instanceof String) {
                ps.setString(idx, (String) value);
            } else if (value instanceof Integer) {
                ps.setInt(idx, (Integer) value);
            } else if (value instanceof Long) {
                ps.setLong(idx, (Long) value);
            } else if (value instanceof Double) {
                ps.setDouble(idx, (Double) value);
            } else if (value instanceof Float) {
                ps.setFloat(idx, (Float) value);
            } else if (value instanceof Date) {
                ps.setTimestamp(idx, new Timestamp(((Date) value).getTime()));
            } else {
                // If nothing matches, just do setObject that will take care of cast value internally based on type
                ps.setObject(idx, value);
            }

            idx++;
        }
    }
}