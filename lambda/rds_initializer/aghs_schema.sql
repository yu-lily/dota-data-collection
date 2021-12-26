CREATE TABLE matches (
    id BIGINT PRIMARY KEY,
    didWin BOOLEAN,
    durationSeconds INTEGER,
    startDateTime TIMESTAMP,
    endDateTime TIMESTAMP,
    clusterId SMALLINT,
    lobbyType SMALLINT,
    numKills SMALLINT,
    numDeaths SMALLINT,
    numHumanPlayers SMALLINT,
    gameMode SMALLINT,
    replaySalt INTEGER,
    difficulty VARCHAR(16),
    depth SMALLINT,
    seed INTEGER,
    battlePoints INTEGER,
    score INTEGER,
    arcaneFragments INTEGER,
    goldBags INTEGER,
    regionId SMALLINT
);

CREATE TABLE players(
    matchId BIGINT,
    playerSlot SMALLINT,
    steamAccountId BIGINT,
    isVictory BOOLEAN,
    heroId SMALLINT,
    deaths SMALLINT,
    leaverStatus SMALLINT,
    numLastHits INTEGER,
    goldPerMinunte SMALLINT,
    networth INTEGER,
    experiencePerMinute SMALLINT,
    level SMALLINT,
    goldSpent INTEGER,
    partyId INTEGER,
    item0Id INTEGER,
    item1Id INTEGER,
    item2Id INTEGER,
    item3Id INTEGER,
    item4Id INTEGER,
    item5Id INTEGER,
    neutral0Id INTEGER,
    arcaneFragments INTEGER,
    bonusArcaneFragments INTEGER,
    goldBags INTEGER,
    neutralItemId INTEGER,
    PRIMARY KEY(matchId, playerSlot),
    FOREIGN KEY(matchId) REFERENCES matches(id)
);

CREATE TABLE playerDepthList(
    matchId BIGINT,
    playerSlot SMALLINT,
    depth SMALLINT,
    steamAccountId BIGINT,
    numDeaths SMALLINT,
    goldBags SMALLINT,
    kills SMALLINT,
    level SMALLINT,
    networth INTEGER,
    rarity SMALLINT,
    selectedRewardAbilityId INTEGER,
    selectedRewardImageAbilityId INTEGER,
    unSelectedRewardAbilityId1 INTEGER,
    unSelectedRewardAbilityId2 INTEGER,
    PRIMARY KEY(matchId, playerSlot, depth),
    FOREIGN KEY(matchId) REFERENCES matches(id),
    FOREIGN KEY(matchId, playerSlot) REFERENCES players(matchId, playerSlot)
);

CREATE TABLE playerBlessings(
    matchId BIGINT,
    playerSlot SMALLINT,
    steamAccountId BIGINT,
    type VARCHAR(64),
    value INTEGER,
    PRIMARY KEY(matchId, playerSlot, type),
    FOREIGN KEY(matchId) REFERENCES matches(id),
    FOREIGN KEY(matchId, playerSlot) REFERENCES players(matchId, playerSlot)
);

CREATE TABLE depthList(
    matchId BIGINT,
    depth SMALLINT,
    selectedElite BOOLEAN,
    selectedEncounter VARCHAR(64),
    selectedEncounterType SMALLINT,
    selectedHidden BOOLEAN,
    selectedReward VARCHAR(64),
    unselectedElite BOOLEAN,
    unselectedEncounter VARCHAR(64),
    unselectedHidden BOOLEAN,
    unselectedReward VARCHAR(64),
    PRIMARY KEY(matchId, depth),
    FOREIGN KEY(matchId) REFERENCES matches(id)
);

CREATE TABLE ascenionAbilities(
    matchId BIGINT,
    depth SMALLINT,
    type VARCHAR(64),
    abilityId SMALLINT,
    modifierId SMALLINT,
    PRIMARY KEY(matchId, depth, type),
    FOREIGN KEY(matchId) REFERENCES matches(id),
    FOREIGN KEY(matchId, depth) REFERENCES depthList(matchId, depth)
);
