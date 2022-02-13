package net.rmnad.minecraft.forge.whitelistsynclib.services;

import net.rmnad.minecraft.forge.whitelistsynclib.callbacks.IOnUserAdd;
import net.rmnad.minecraft.forge.whitelistsynclib.callbacks.IOnUserRemove;
import net.rmnad.minecraft.forge.whitelistsynclib.models.OppedPlayer;
import net.rmnad.minecraft.forge.whitelistsynclib.models.WhitelistedPlayer;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Interface for different database services
 * @author Richard Nader, Jr. <rmnader@svsu.edu>
 */
public interface BaseService {

    public boolean initializeDatabase();
    public boolean requiresSyncing();

    // Getter functions
    public ArrayList<WhitelistedPlayer> getWhitelistedPlayersFromDatabase();
    public ArrayList<OppedPlayer> getOppedPlayersFromDatabase();

    // Syncing functions
    public boolean copyLocalWhitelistedPlayersToDatabase(ArrayList<WhitelistedPlayer> whitelistedPlayers);
    public boolean copyLocalOppedPlayersToDatabase(ArrayList<OppedPlayer> oppedPlayers);

    public boolean copyDatabaseWhitelistedPlayersToLocal(ArrayList<WhitelistedPlayer> localWhitelistedPlayers, IOnUserAdd onUserAdd, IOnUserRemove onUserRemove);
    public boolean copyDatabaseOppedPlayersToLocal(ArrayList<OppedPlayer> localOppedPlayers, IOnUserAdd onUserAdd, IOnUserRemove onUserRemove);


    // Addition functions
    public boolean addWhitelistPlayer(UUID uuid, String name);
    public boolean addOppedPlayer(UUID uuid, String name);


    // Removal functions
    public boolean removeWhitelistPlayer(UUID uuid, String name);
    public boolean removeOppedPlayer(UUID uuid, String name);

}
