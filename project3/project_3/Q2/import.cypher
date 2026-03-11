// 1. Καθαρισμός
MATCH (n) DETACH DELETE n;

// Indexes
CREATE INDEX play_idx IF NOT EXISTS FOR (p:Play) ON (p.title);
CREATE INDEX char_idx IF NOT EXISTS FOR (c:Character) ON (c.name, c.play);
CREATE INDEX scene_idx IF NOT EXISTS FOR (s:Scene) ON (s.play, s.act, s.name); // Προσοχή: s.name όχι s.scene
CREATE INDEX act_idx IF NOT EXISTS FOR (a:Act) ON (a.play, a.name);

// 2. Load Plays
LOAD CSV WITH HEADERS FROM 'file:///plays.csv' AS row
WITH row WHERE row.title IS NOT NULL
MERGE (p:Play {title: trim(row.title)});

// 3. Load Scenes & Acts
LOAD CSV WITH HEADERS FROM 'file:///scenes.csv' AS row
MATCH (p:Play {title: trim(row.play)})
MERGE (a:Act {
    name: toUpper(trim(row.act)),
    play: p.title
})
MERGE (p)-[:HAS_ACT]->(a)
MERGE (s:Scene {
    name: toUpper(trim(row.scene)),
    act: toUpper(trim(row.act)),
    play: p.title
})
MERGE (a)-[:HAS_SCENE]->(s);

// 4. Load Characters
LOAD CSV WITH HEADERS FROM 'file:///characters.csv' AS row
MERGE (c:Character {name: trim(row.name), play: trim(row.play)});

// 5. Load Lines
LOAD CSV WITH HEADERS FROM 'file:///lines.csv' AS row
MATCH (s:Scene {
    name: toUpper(trim(row.scene)), 
    act: toUpper(trim(row.act)), 
    play: trim(row.play)
})
MATCH (c:Character {
    name: trim(row.character), 
    play: trim(row.play)
})

CREATE (l:Line {
    lineNum: toInteger(row.line_number), 
    text: row.text, 
    play: trim(row.play), 
    act: toUpper(trim(row.act)), 
    scene: toUpper(trim(row.scene))
})

CREATE (s)-[:HAS_LINE]->(l)
CREATE (c)-[:SPEAKS]->(l);