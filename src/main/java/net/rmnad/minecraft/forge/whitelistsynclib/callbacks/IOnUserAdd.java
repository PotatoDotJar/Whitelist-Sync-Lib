package net.rmnad.minecraft.forge.whitelistsynclib.callbacks;

import java.util.UUID;

public interface IOnUserAdd {
    void call(UUID uuid, String name);
}
