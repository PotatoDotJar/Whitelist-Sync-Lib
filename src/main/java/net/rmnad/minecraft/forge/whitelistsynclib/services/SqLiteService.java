package net.rmnad.minecraft.forge.whitelistsynclib.services;


import net.rmnad.minecraft.forge.whitelistsynclib.WhitelistSyncLib;
import net.rmnad.minecraft.forge.whitelistsynclib.callbacks.IOnUserAdd;
import net.rmnad.minecraft.forge.whitelistsynclib.callbacks.IOnUserRemove;
import net.rmnad.minecraft.forge.whitelistsynclib.models.OppedPlayer;
import net.rmnad.minecraft.forge.whitelistsynclib.models.WhitelistedPlayer;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Service for SQLITE Databases
 *
 * @author Richard Nader, Jr. <rmnader@svsu.edu>
 */
public class SqLiteService implements BaseService {

    private final boolean syncingOpList;
    private final String databasePath;
    
    public SqLiteService(String databasePath, boolean syncingOpList) {
        this.databasePath = databasePath;
        this.syncingOpList = syncingOpList;
    }

    @Override
    public boolean requiresSyncing() {
        return false;
    }

    // Function used to initialize the database file
    @Override
    public boolean initializeDatabase() {
        WhitelistSyncLib.LOGGER.info("Setting up the SQLite service...");
        File databaseFile = new File(this.databasePath);
        boolean isSuccess = true;

        try {
            Class.forName("org.sqlite.JDBC").getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            WhitelistSyncLib.LOGGER.error("Failed to init sqlite connector. Is the library missing?");
            WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
            isSuccess = false;
        }

        // If database does not exist, create a new one
        if (!databaseFile.exists() && isSuccess) {
            String url = "jdbc:sqlite:" + this.databasePath;
            try {
                Connection conn = DriverManager.getConnection(url);

                WhitelistSyncLib.LOGGER.info("A new database \"" + this.databasePath + "\" has been created.");
                conn.close();
            } catch (SQLException e) {
                // Something is wrong...
                WhitelistSyncLib.LOGGER.error("Failed to create new SQLite database file!");
                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
                isSuccess = false;
            }
        }

        // Create whitelist table if it doesn't exist.
        if (isSuccess) {
            try {
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);

                // If the conn is valid, everything below this will run
                WhitelistSyncLib.LOGGER.info("Connected to SQLite database successfully!");

                // SQL statement for creating a new table
                String sql = "CREATE TABLE IF NOT EXISTS whitelist (\n"
                        + "	uuid text NOT NULL PRIMARY KEY,\n"
                        + "	name text,\n"
                        + " whitelisted integer NOT NULL);";
                Statement stmt = conn.createStatement();
                stmt.execute(sql);
                stmt.close();

                if (this.syncingOpList) {
                    // SQL statement for creating a new table
                    sql = "CREATE TABLE IF NOT EXISTS op (\n"
                            + "	uuid text NOT NULL PRIMARY KEY,\n"
                            + "	name text,\n"
                            + " isOp integer NOT NULL);";
                    Statement stmt2 = conn.createStatement();
                    stmt2.execute(sql);
                    stmt2.close();
                }

                conn.close();
            } catch (SQLException e) {
                // Something is wrong...
                WhitelistSyncLib.LOGGER.error("Error creating op or whitelist table!\n" + e.getMessage());
                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
                isSuccess = false;
            }
        }

        return isSuccess;
    }

    @Override
    public ArrayList<WhitelistedPlayer> getWhitelistedPlayersFromDatabase() {
        // ArrayList for whitelisted players.
        ArrayList<WhitelistedPlayer> whitelistedPlayers = new ArrayList<>();

        try {
            // Keep track of records.
            int records = 0;

            // Connect to database.
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);
            long startTime = System.currentTimeMillis();

            String sql = "SELECT uuid, name, whitelisted FROM whitelist WHERE whitelisted = 1;";
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();

            // Save queried return to names list.
            while (rs.next()) {
                whitelistedPlayers.add(new WhitelistedPlayer(rs.getString("uuid"), rs.getString("name"), true));
                records++;
            }

            // Total time taken.
            long timeTaken = System.currentTimeMillis() - startTime;

            WhitelistSyncLib.LOGGER.debug("Database pulled whitelisted players | Took " + timeTaken + "ms | Read " + records + " records.");

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            WhitelistSyncLib.LOGGER.error("Error querying whitelisted players from database!");
            WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
        }

        return whitelistedPlayers;
    }

    @Override
    public ArrayList<OppedPlayer> getOppedPlayersFromDatabase() {
        // ArrayList for opped players.
        ArrayList<OppedPlayer> oppedPlayers = new ArrayList<>();

        if (this.syncingOpList) {
            try {
                // Keep track of records.
                int records = 0;

                // Connect to database.
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);
                long startTime = System.currentTimeMillis();

                String sql = "SELECT uuid, name FROM op WHERE isOp = 1;";
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();

                // Save queried return to names list.
                while (rs.next()) {
                    oppedPlayers.add(new OppedPlayer(rs.getString("uuid"), rs.getString("name"), true));
                    records++;
                }

                // Total time taken.
                long timeTaken = System.currentTimeMillis() - startTime;

                WhitelistSyncLib.LOGGER.debug("Database pulled opped players | Took " + timeTaken + "ms | Read " + records + " records.");

                stmt.close();
                conn.close();
            } catch (SQLException e) {
                WhitelistSyncLib.LOGGER.error("Error querying opped players from database!");
                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
            }

        } else {
            WhitelistSyncLib.LOGGER.error("Op list syncing is currently disabled in your config. "
                    + "Please enable it and restart the server to use this feature.");
        }

        return oppedPlayers;
    }

    @Override
    public boolean copyLocalWhitelistedPlayersToDatabase(ArrayList<WhitelistedPlayer> whitelistedPlayers) {
        // TODO: Start job on thread to avoid lag?
        // Keep track of records.
        int records = 0;
        try {
            // Connect to database.
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);
            long startTime = System.currentTimeMillis();
            // Loop through local whitelist and insert into database.
            for (WhitelistedPlayer player : whitelistedPlayers) {

                if (player.getUuid() != null && player.getName() != null) {
                    PreparedStatement stmt = conn.prepareStatement("INSERT OR REPLACE INTO whitelist(uuid, name, whitelisted) VALUES (?, ?, 1)");
                    stmt.setString(1, player.getUuid());
                    stmt.setString(2, player.getName());
                    stmt.executeUpdate();
                    stmt.close();

                    records++;
                }
            }
            // Record time taken.
            long timeTaken = System.currentTimeMillis() - startTime;
            WhitelistSyncLib.LOGGER.debug("Whitelist table updated | Took " + timeTaken + "ms | Wrote " + records + " records.");
            conn.close();

            return true;
        } catch (SQLException e) {
            WhitelistSyncLib.LOGGER.error("Failed to update database with local records.");
            WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
        }

        return false;
    }

    @Override
    public boolean copyLocalOppedPlayersToDatabase(ArrayList<OppedPlayer> oppedPlayers) {
        if (this.syncingOpList) {
            // TODO: Start job on thread to avoid lag?
            // Keep track of records.
            int records = 0;
            try {
                // Connect to database.
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);
                long startTime = System.currentTimeMillis();
                // Loop through local opped players and insert into database.
                for (OppedPlayer player : oppedPlayers) {

                    if (player.getUuid() != null && player.getName() != null) {
                        PreparedStatement stmt = conn.prepareStatement("INSERT OR REPLACE INTO op(uuid, name, isOp) VALUES (?, ?, 1)");
                        stmt.setString(1, player.getUuid());
                        stmt.setString(2, player.getName());
                        stmt.executeUpdate();
                        stmt.close();

                        records++;
                    }
                }
                // Record time taken.
                long timeTaken = System.currentTimeMillis() - startTime;
                WhitelistSyncLib.LOGGER.debug("Op table updated | Took " + timeTaken + "ms | Wrote " + records + " records.");
                conn.close();

                return true;
            } catch (SQLException e) {
                WhitelistSyncLib.LOGGER.error("Failed to update database with local records.");
                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
            }
        } else {
            WhitelistSyncLib.LOGGER.error("Op list syncing is currently disabled in your config. "
                    + "Please enable it and restart the server to use this feature.");
        }

        return false;
    }

    @Override
    public boolean copyDatabaseWhitelistedPlayersToLocal(ArrayList<WhitelistedPlayer> localWhitelistedPlayers, IOnUserAdd onUserAdd, IOnUserRemove onUserRemove) {
        try {
            int records = 0;

            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);
            long startTime = System.currentTimeMillis();

            String sql = "SELECT name, uuid, whitelisted FROM whitelist;";
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                UUID uuid = UUID.fromString(rs.getString("uuid"));
                String name = rs.getString("name");
                int whitelisted = rs.getInt("whitelisted");

                if (whitelisted == 1) {
                    if (localWhitelistedPlayers.stream().noneMatch(o -> o.getUuid().equals(uuid))) {
                        try {
                            onUserAdd.call(uuid, name);
                            WhitelistSyncLib.LOGGER.debug("Added " + name + " to whitelist.");
                            records++;
                        } catch (NullPointerException e) {
                            WhitelistSyncLib.LOGGER.error("Player is null?");
                            WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
                        }
                    }
                } else {
                    if (localWhitelistedPlayers.stream().anyMatch(o -> o.getUuid().equals(uuid))) {
                        onUserRemove.call(uuid, name);
                        WhitelistSyncLib.LOGGER.debug("Removed " + name + " from whitelist.");
                        records++;
                    }
                }

            }
            long timeTaken = System.currentTimeMillis() - startTime;
            WhitelistSyncLib.LOGGER.debug("Copied whitelist database to local | Took " + timeTaken + "ms | Wrote " + records + " records.");

            stmt.close();
            conn.close();
            return true;

        } catch (SQLException e) {
            WhitelistSyncLib.LOGGER.error("Error querying whitelisted players from database!");
            WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
        }

        return false;
    }

    @Override
    public boolean copyDatabaseOppedPlayersToLocal(ArrayList<OppedPlayer> localOppedPlayers, IOnUserAdd onUserAdd, IOnUserRemove onUserRemove) {

        if (this.syncingOpList) {

            try {
                int records = 0;

                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);
                long startTime = System.currentTimeMillis();

                String sql = "SELECT name, uuid, isOp FROM op;";
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    UUID uuid = UUID.fromString(rs.getString("uuid"));
                    String name = rs.getString("name");
                    int opped = rs.getInt("isOp");

                    if (opped == 1) {
                        if (localOppedPlayers.stream().noneMatch(o -> o.getUuid().equals(uuid))) {
                            try {
                                onUserAdd.call(uuid, name);
                                WhitelistSyncLib.LOGGER.debug("Opped " + name + ".");
                                records++;
                            } catch (NullPointerException e) {
                                WhitelistSyncLib.LOGGER.error("Player is null?");
                                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
                            }
                        }
                    } else {
                        if (localOppedPlayers.stream().anyMatch(o -> o.getUuid().equals(uuid))) {
                            onUserRemove.call(uuid, name);
                            WhitelistSyncLib.LOGGER.debug("Deopped " + name + ".");
                            records++;
                        }
                    }
                }
                long timeTaken = System.currentTimeMillis() - startTime;
                WhitelistSyncLib.LOGGER.debug("Copied op database to local | Took " + timeTaken + "ms | Wrote " + records + " records.");

                stmt.close();
                conn.close();
                return true;
            } catch (SQLException e) {
                WhitelistSyncLib.LOGGER.error("Error querying opped players from database!");
                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
            }
        } else {
            WhitelistSyncLib.LOGGER.error("Op list syncing is currently disabled in your config. "
                    + "Please enable it and restart the server to use this feature.");
        }

        return false;
    }

    @Override
    public boolean addWhitelistPlayer(UUID uuid, String name) {
        try {
            // Open connection
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);

            // Start time.
            long startTime = System.currentTimeMillis();

            String sql = "INSERT OR REPLACE INTO whitelist(uuid, name, whitelisted) VALUES (?, ?, 1)";
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, uuid.toString());
            stmt.setString(2, name);
            stmt.executeUpdate();

            // Time taken.
            long timeTaken = System.currentTimeMillis() - startTime;
            WhitelistSyncLib.LOGGER.debug("Added " + name + " to whitelist | Took " + timeTaken + "ms");
            stmt.close();
            conn.close();
            return true;

        } catch (SQLException e) {
            WhitelistSyncLib.LOGGER.error("Error adding " + name + " to whitelist database!");
            WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
        }

        return false;
    }

    @Override
    public boolean addOppedPlayer(UUID uuid, String name) {
        if (this.syncingOpList) {
            try {
                // Open connection
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);

                // Start time.
                long startTime = System.currentTimeMillis();

                PreparedStatement stmt = conn.prepareStatement("INSERT OR REPLACE INTO op(uuid, name, isOp) VALUES (?, ?, 1)");
                stmt.setString(1, uuid.toString());
                stmt.setString(2, name);
                stmt.executeUpdate();

                // Time taken.
                long timeTaken = System.currentTimeMillis() - startTime;
                WhitelistSyncLib.LOGGER.debug("Database opped " + name + " | Took " + timeTaken + "ms");
                stmt.close();
                conn.close();
                return true;

            } catch (SQLException e) {
                WhitelistSyncLib.LOGGER.error("Error opping " + name + " !");
                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
            }
        } else {
            WhitelistSyncLib.LOGGER.error("Op list syncing is currently disabled in your config. "
                    + "Please enable it and restart the server to use this feature.");
        }

        return false;
    }

    @Override
    public boolean removeWhitelistPlayer(UUID uuid, String name) {
        try {
            // Open connection
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);

            // Start time.
            long startTime = System.currentTimeMillis();

            PreparedStatement stmt = conn.prepareStatement("INSERT OR REPLACE INTO whitelist(uuid, name, whitelisted) VALUES (?, ?, 0)");
            stmt.setString(1, uuid.toString());
            stmt.setString(2, name);
            stmt.executeUpdate();

            // Time taken.
            long timeTaken = System.currentTimeMillis() - startTime;
            WhitelistSyncLib.LOGGER.debug("Removed " + name + " from whitelist | Took " + timeTaken + "ms");
            stmt.close();
            conn.close();
            return true;

        } catch (SQLException e) {
            WhitelistSyncLib.LOGGER.error("Error removing " + name + " to whitelist database!");
            WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
        }

        return false;
    }

    @Override
    public boolean removeOppedPlayer(UUID uuid, String name) {
        if (this.syncingOpList) {
            try {
                // Open connection
                Connection conn = DriverManager.getConnection("jdbc:sqlite:" + this.databasePath);

                // Start time.
                long startTime = System.currentTimeMillis();

                PreparedStatement stmt = conn.prepareStatement("INSERT OR REPLACE INTO op(uuid, name, isOp) VALUES (?, ?, 0)");
                stmt.setString(1, uuid.toString());
                stmt.setString(2, name);
                stmt.executeUpdate();

                // Time taken
                long timeTaken = System.currentTimeMillis() - startTime;
                WhitelistSyncLib.LOGGER.debug("Deopped " + name + " | Took " + timeTaken + "ms");
                stmt.close();
                conn.close();
                return true;

            } catch (SQLException e) {
                WhitelistSyncLib.LOGGER.error("Error deopping " + name + ".");
                WhitelistSyncLib.LOGGER.error(e.getMessage(), e);
            }
        } else {
            WhitelistSyncLib.LOGGER.error("Op list syncing is currently disabled in your config. "
                    + "Please enable it and restart the server to use this feature.");
        }

        return false;
    }
}
