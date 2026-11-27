use ferrite_protocol::QoS;
use std::collections::HashMap;
use std::fmt;
use tracing::warn;

const MAX_FILTER_BYTES: usize = 65535;
const MAX_FILTER_LEVELS: usize = 64;

/// Errore di validazione di un MQTT Topic Filter (MQTT-4.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterError {
    Empty,
    TooLong,
    TooDeep,
    InvalidHash,
    InvalidPlus,
    NulChar,
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterError::Empty => write!(f, "topic filter cannot be empty"),
            FilterError::TooLong => {
                write!(
                    f,
                    "topic filter exceeds maximum length of {MAX_FILTER_BYTES} bytes"
                )
            }
            FilterError::TooDeep => {
                write!(
                    f,
                    "topic filter exceeds maximum depth of {MAX_FILTER_LEVELS} levels"
                )
            }
            FilterError::InvalidHash => {
                write!(
                    f,
                    "'#' wildcard must occupy an entire level and be the last level"
                )
            }
            FilterError::InvalidPlus => {
                write!(f, "'+' wildcard must occupy an entire level")
            }
            FilterError::NulChar => write!(f, "topic filter cannot contain NUL (U+0000)"),
        }
    }
}

impl std::error::Error for FilterError {}

/// Valida un MQTT Topic Filter secondo le regole MQTT 3.1.1 (sezione 4.7) e i limiti del broker.
pub fn validate_filter(filter: &str) -> Result<(), FilterError> {
    if filter.is_empty() {
        return Err(FilterError::Empty);
    }

    let mut levels = 0usize;
    let mut iter = filter.split('/').peekable();
    while let Some(token) = iter.next() {
        levels += 1;
        if levels > MAX_FILTER_LEVELS {
            return Err(FilterError::TooDeep);
        }
        if token.contains('\0') {
            return Err(FilterError::NulChar);
        }
        if token.contains('#') && (token != "#" || iter.peek().is_some()) {
            return Err(FilterError::InvalidHash);
        }
        if token.contains('+') && token != "+" {
            return Err(FilterError::InvalidPlus);
        }
    }

    if filter.len() > MAX_FILTER_BYTES {
        return Err(FilterError::TooLong);
    }

    Ok(())
}

/// Errore di validazione di un MQTT Topic Name usato in `PUBLISH` (MQTT-3.3.2 / MQTT-4.7.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicNameError {
    Empty,
    TooLong,
    Wildcard,
    NulChar,
}

impl fmt::Display for TopicNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopicNameError::Empty => write!(f, "topic name cannot be empty"),
            TopicNameError::TooLong => {
                write!(
                    f,
                    "topic name exceeds maximum length of {MAX_FILTER_BYTES} bytes"
                )
            }
            TopicNameError::Wildcard => {
                write!(
                    f,
                    "topic name cannot contain wildcard characters '+' or '#'"
                )
            }
            TopicNameError::NulChar => write!(f, "topic name cannot contain NUL (U+0000)"),
        }
    }
}

impl std::error::Error for TopicNameError {}

/// Valida un MQTT Topic Name concreto (usato in `PUBLISH`, MQTT-3.3.2-2 e MQTT-4.7.3-1).
pub fn validate_topic_name(topic: &str) -> Result<(), TopicNameError> {
    if topic.is_empty() {
        return Err(TopicNameError::Empty);
    }
    if topic.len() > MAX_FILTER_BYTES {
        return Err(TopicNameError::TooLong);
    }
    if topic.contains('\0') {
        return Err(TopicNameError::NulChar);
    }
    if topic.contains('+') || topic.contains('#') {
        return Err(TopicNameError::Wildcard);
    }
    Ok(())
}

/// Rappresenta una sottoscrizione effettuata da un client a un topic filter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub client_id: String,
    pub qos: QoS,
}

/// Nodo interno dell'albero gerarchico di routing.
#[derive(Debug, Default)]
pub struct TrieNode {
    /// Client sottoscritti esattamente a questo nodo di topic concreto.
    subscribers: HashMap<String, QoS>,
    /// Sottomappa per token di topic specifici (es. "sensors", "room1").
    children: HashMap<String, TrieNode>,
    /// Ramo speciale per il wildcard di singolo livello (+).
    plus_child: Option<Box<TrieNode>>,
    /// Subscriber registrati sul multi-level wildcard (#) a questo livello.
    hash_subscribers: HashMap<String, QoS>,
}

impl TrieNode {
    fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
            && self.children.is_empty()
            && self.plus_child.is_none()
            && self.hash_subscribers.is_empty()
    }

    #[cfg(test)]
    fn node_count(&self) -> usize {
        let mut count = 1;
        for child in self.children.values() {
            count += child.node_count();
        }
        if let Some(plus) = self.plus_child.as_deref() {
            count += plus.node_count();
        }
        count
    }
}

#[derive(Debug, Default)]
pub struct TopicTrie {
    root: TrieNode,
}

impl TopicTrie {
    pub fn new() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }

    /// Inserisce un filtro di sottoscrizione validato nel trie.
    pub fn try_subscribe(
        &mut self,
        topic_filter: &str,
        client_id: String,
        qos: QoS,
    ) -> Result<(), FilterError> {
        validate_filter(topic_filter)?;

        let tokens: Vec<&str> = topic_filter.split('/').collect();
        let mut current = &mut self.root;

        for (idx, &token) in tokens.iter().enumerate() {
            if token == "#" {
                // '#' deve essere l'ultimo carattere del filtro
                current.hash_subscribers.insert(client_id, qos);
                return Ok(());
            } else if token == "+" {
                if current.plus_child.is_none() {
                    current.plus_child = Some(Box::new(TrieNode::default()));
                }
                current = current.plus_child.as_deref_mut().unwrap();
            } else {
                current = current.children.entry(token.to_string()).or_default();
            }

            // Se siamo all'ultimo livello e non era '#', registriamo la sottoscrizione
            if idx == tokens.len() - 1 {
                current.subscribers.insert(client_id.clone(), qos);
            }
        }

        Ok(())
    }

    /// Inserisce un filtro di sottoscrizione (es. "sensors/+/temperature") per un dato client.
    /// Se il filtro è invalido non inserisce nulla e registra un `warn!`.
    pub fn subscribe(&mut self, topic_filter: &str, client_id: String, qos: QoS) {
        if let Err(err) = self.try_subscribe(topic_filter, client_id, qos) {
            warn!("Ignoring invalid topic filter {:?}: {}", topic_filter, err);
        }
    }
    /// Rimuove la sottoscrizione di un client a uno specifico topic filter.
    /// Se il filtro è invalido non modifica il trie e registra un `warn!`.
    pub fn unsubscribe(&mut self, topic_filter: &str, client_id: &str) {
        if let Err(err) = validate_filter(topic_filter) {
            warn!(
                "Ignoring invalid unsubscribe topic filter {:?}: {}",
                topic_filter, err
            );
            return;
        }
        let tokens: Vec<&str> = topic_filter.split('/').collect();
        Self::remove_recursive(&mut self.root, &tokens, 0, client_id);
    }

    fn remove_recursive(node: &mut TrieNode, tokens: &[&str], idx: usize, client_id: &str) -> bool {
        if idx < tokens.len() {
            let token = tokens[idx];
            if token == "#" {
                node.hash_subscribers.remove(client_id);
            } else if token == "+" {
                let prune_plus = node
                    .plus_child
                    .as_deref_mut()
                    .is_some_and(|plus| Self::remove_recursive(plus, tokens, idx + 1, client_id));
                if prune_plus {
                    node.plus_child = None;
                }
            } else if let Some(child) = node.children.get_mut(token) {
                if Self::remove_recursive(child, tokens, idx + 1, client_id) {
                    node.children.remove(token);
                }
            }
        } else {
            node.subscribers.remove(client_id);
        }

        node.is_empty()
    }

    /// Rimuove *tutte* le sottoscrizioni di un client, senza conoscere i suoi filtri.
    ///
    /// Visita l'intero albero (costo proporzionale alla sua dimensione), quindi va usata
    /// solo nei casi rari, ad esempio quando un nuovo CONNECT sostituisce una sessione
    /// esistente. Nel caso normale `unsubscribe` per filtro è molto più economico.
    pub fn remove_client(&mut self, client_id: &str) {
        Self::remove_client_recursive(&mut self.root, client_id);
    }

    fn remove_client_recursive(node: &mut TrieNode, client_id: &str) -> bool {
        node.subscribers.remove(client_id);
        node.hash_subscribers.remove(client_id);
        node.children
            .retain(|_, child| !Self::remove_client_recursive(child, client_id));
        if let Some(plus) = node.plus_child.as_deref_mut() {
            if Self::remove_client_recursive(plus, client_id) {
                node.plus_child = None;
            }
        }
        node.is_empty()
    }

    #[cfg(test)]
    fn node_count(&self) -> usize {
        self.root.node_count()
    }

    /// Trova tutti i client a cui recapitare un messaggio pubblicato su un topic concreto.
    /// Esempio: publish su "sensors/livingroom/temp" trova chi ascolta su:
    /// - "sensors/livingroom/temp"
    /// - "sensors/+/temp"
    /// - "sensors/#"
    /// - "#"
    pub fn match_topic(&self, concrete_topic: &str) -> Vec<Subscription> {
        let tokens: Vec<&str> = concrete_topic.split('/').collect();
        let mut matched = HashMap::new();

        Self::match_recursive(&self.root, &tokens, 0, &mut matched);

        matched
            .into_iter()
            .map(|(client_id, qos)| Subscription { client_id, qos })
            .collect()
    }

    fn match_recursive(
        node: &TrieNode,
        tokens: &[&str],
        idx: usize,
        results: &mut HashMap<String, QoS>,
    ) {
        // MQTT-4.7.2-1: un topic che inizia con '$' non deve matchare i wildcard '#' o '+' in radice.
        let skip_root_wildcards = idx == 0 && tokens.first().is_some_and(|t| t.starts_with('$'));

        // 1. Chiunque sia sottoscritto con '#' a questo livello matcha tutto ciò che segue
        if !skip_root_wildcards {
            for (client_id, &qos) in &node.hash_subscribers {
                results
                    .entry(client_id.clone())
                    .and_modify(|existing_qos| {
                        if qos as u8 > *existing_qos as u8 {
                            *existing_qos = qos;
                        }
                    })
                    .or_insert(qos);
            }
        }

        // 2. Se abbiamo esaminato tutti i token del topic concreto
        if idx == tokens.len() {
            for (client_id, &qos) in &node.subscribers {
                results
                    .entry(client_id.clone())
                    .and_modify(|existing_qos| {
                        if qos as u8 > *existing_qos as u8 {
                            *existing_qos = qos;
                        }
                    })
                    .or_insert(qos);
            }
            return;
        }

        let token = tokens[idx];

        // 3. Match esatto del token
        if let Some(child) = node.children.get(token) {
            Self::match_recursive(child, tokens, idx + 1, results);
        }

        // 4. Match tramite wildcard '+'
        if !skip_root_wildcards {
            if let Some(plus) = node.plus_child.as_deref() {
                Self::match_recursive(plus, tokens, idx + 1, results);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_topic_match() {
        let mut trie = TopicTrie::new();
        trie.subscribe("a/b/c", "client1".into(), QoS::AtMostOnce);
        trie.subscribe("a/b/d", "client2".into(), QoS::AtLeastOnce);

        let matches = trie.match_topic("a/b/c");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "client1");
    }

    #[test]
    fn test_wildcard_plus_match() {
        let mut trie = TopicTrie::new();
        trie.subscribe("sensors/+/temp", "dashboard".into(), QoS::AtMostOnce);

        let matches1 = trie.match_topic("sensors/kitchen/temp");
        assert_eq!(matches1.len(), 1);
        assert_eq!(matches1[0].client_id, "dashboard");

        let matches2 = trie.match_topic("sensors/bedroom/temp");
        assert_eq!(matches2.len(), 1);

        let nomatch = trie.match_topic("sensors/kitchen/humidity");
        assert!(nomatch.is_empty());
    }

    #[test]
    fn test_wildcard_hash_match() {
        let mut trie = TopicTrie::new();
        trie.subscribe("devices/#", "logger".into(), QoS::AtLeastOnce);

        assert_eq!(trie.match_topic("devices/gateway1").len(), 1);
        assert_eq!(trie.match_topic("devices/gateway1/status").len(), 1);
        assert_eq!(trie.match_topic("devices/g1/sensor2/val").len(), 1);
        assert!(trie.match_topic("other/topic").is_empty());
    }

    #[test]
    fn test_unsubscribe() {
        let mut trie = TopicTrie::new();
        trie.subscribe("home/+", "user".into(), QoS::AtMostOnce);
        assert_eq!(trie.match_topic("home/light").len(), 1);

        trie.unsubscribe("home/+", "user");
        assert!(trie.match_topic("home/light").is_empty());
    }

    #[test]
    fn test_remove_client_removes_every_subscription() {
        let mut trie = TopicTrie::new();
        trie.subscribe("a/b", "gone".into(), QoS::AtMostOnce);
        trie.subscribe("a/+/c", "gone".into(), QoS::AtLeastOnce);
        trie.subscribe("x/#", "gone".into(), QoS::AtMostOnce);
        trie.subscribe("a/b", "stays".into(), QoS::AtMostOnce);

        trie.remove_client("gone");

        let matches = trie.match_topic("a/b");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "stays");
        assert!(trie.match_topic("a/z/c").is_empty());
        assert!(trie.match_topic("x/y/z").is_empty());
    }

    #[test]
    fn test_validate_filter_valid_cases() {
        for filter in ["#", "+", "a/b", "a/+/c", "a/#", "/a", "a/", "+/+"] {
            assert_eq!(
                validate_filter(filter),
                Ok(()),
                "filtro valido rifiutato: {filter:?}"
            );
        }
    }

    #[test]
    fn test_validate_filter_error_variants() {
        assert_eq!(validate_filter(""), Err(FilterError::Empty));
        assert_eq!(
            validate_filter(&"a".repeat(MAX_FILTER_BYTES + 1)),
            Err(FilterError::TooLong)
        );
        assert_eq!(
            validate_filter(&vec!["a"; MAX_FILTER_LEVELS + 1].join("/")),
            Err(FilterError::TooDeep)
        );
        assert_eq!(validate_filter("a/#/b"), Err(FilterError::InvalidHash));
        assert_eq!(validate_filter("a#"), Err(FilterError::InvalidHash));
        assert_eq!(validate_filter("#a"), Err(FilterError::InvalidHash));
        assert_eq!(validate_filter("a+"), Err(FilterError::InvalidPlus));
        assert_eq!(validate_filter("+a"), Err(FilterError::InvalidPlus));
        assert_eq!(validate_filter("a/\0/b"), Err(FilterError::NulChar));
    }

    #[test]
    fn test_depth_limit_64_valid_and_100000_rejected() {
        let mut trie = TopicTrie::new();

        // 64 livelli: valido, inseribile, matchabile e rimovibile senza crash.
        let max_valid = vec!["a"; MAX_FILTER_LEVELS].join("/");
        assert_eq!(
            trie.try_subscribe(&max_valid, "c1".into(), QoS::AtMostOnce),
            Ok(())
        );
        assert_eq!(trie.match_topic(&max_valid).len(), 1);
        trie.unsubscribe(&max_valid, "c1");
        assert!(trie.match_topic(&max_valid).is_empty());

        // 100000 livelli: rifiutato subito con TooDeep senza crash.
        let deep = "a/".repeat(100_000);
        assert_eq!(
            trie.try_subscribe(&deep, "c2".into(), QoS::AtMostOnce),
            Err(FilterError::TooDeep)
        );
    }

    #[test]
    fn test_unsubscribe_prunes_empty_nodes() {
        let mut trie = TopicTrie::new();
        assert_eq!(trie.node_count(), 1);

        let filters: Vec<String> = (0..1000)
            .map(|i| match i % 3 {
                0 => format!("devices/dev-{i}/status"),
                1 => format!("sensors/{i}/+/temp"),
                _ => format!("logs/{i}/#"),
            })
            .collect();

        for filter in &filters {
            trie.subscribe(filter, "client1".into(), QoS::AtMostOnce);
        }
        assert!(trie.node_count() > 1000);

        for filter in &filters {
            trie.unsubscribe(filter, "client1");
        }
        assert_eq!(trie.node_count(), 1);
    }

    #[test]
    fn test_remove_client_prunes_empty_nodes() {
        let mut trie = TopicTrie::new();
        assert_eq!(trie.node_count(), 1);

        for i in 0..1000 {
            let filter = match i % 3 {
                0 => format!("devices/dev-{i}/status"),
                1 => format!("sensors/{i}/+/temp"),
                _ => format!("logs/{i}/#"),
            };
            trie.subscribe(&filter, "client1".into(), QoS::AtMostOnce);
        }
        assert!(trie.node_count() > 1000);

        trie.remove_client("client1");
        assert_eq!(trie.node_count(), 1);
    }

    #[test]
    fn test_dollar_topics_do_not_match_root_wildcards() {
        let mut trie = TopicTrie::new();
        trie.subscribe("#", "root_hash".into(), QoS::AtMostOnce);
        trie.subscribe("+/x", "root_plus".into(), QoS::AtMostOnce);
        trie.subscribe("$SYS/#", "sys_hash".into(), QoS::AtMostOnce);
        trie.subscribe("$SYS/+", "sys_plus".into(), QoS::AtMostOnce);

        // MQTT-4.7.2-1: '#' e '+/x' non ricevono '$SYS/x'; '$SYS/#' e '$SYS/+' sì.
        let mut matches_x: Vec<String> = trie
            .match_topic("$SYS/x")
            .into_iter()
            .map(|s| s.client_id)
            .collect();
        matches_x.sort();
        assert_eq!(matches_x, vec!["sys_hash", "sys_plus"]);

        // '$SYS/broker/load' matcha solo '$SYS/#' (non '#' in radice, non '$SYS/+').
        let matches_load: Vec<String> = trie
            .match_topic("$SYS/broker/load")
            .into_iter()
            .map(|s| s.client_id)
            .collect();
        assert_eq!(matches_load, vec!["sys_hash"]);

        // Un topic normale matcha comunque '#' e '+/x'.
        let mut matches_normal: Vec<String> = trie
            .match_topic("normal/x")
            .into_iter()
            .map(|s| s.client_id)
            .collect();
        matches_normal.sort();
        assert_eq!(matches_normal, vec!["root_hash", "root_plus"]);
    }

    #[test]
    fn test_wildcard_hash_matches_parent_level() {
        // MQTT-4.7.1-2: 'a/#' include anche il livello padre 'a'.
        let mut trie = TopicTrie::new();
        trie.subscribe("a/#", "sub".into(), QoS::AtMostOnce);

        assert_eq!(trie.match_topic("a").len(), 1);
        assert_eq!(trie.match_topic("a/").len(), 1);
        assert_eq!(trie.match_topic("a/b").len(), 1);
        assert!(trie.match_topic("ab").is_empty());
    }

    #[test]
    fn test_validate_topic_name() {
        for topic in ["a", "a/b", "/a", "a/", "$SYS/broker/load"] {
            assert_eq!(
                validate_topic_name(topic),
                Ok(()),
                "topic valido rifiutato: {topic:?}"
            );
        }
        assert_eq!(validate_topic_name(""), Err(TopicNameError::Empty));
        assert_eq!(
            validate_topic_name(&"a".repeat(MAX_FILTER_BYTES + 1)),
            Err(TopicNameError::TooLong)
        );
        assert_eq!(validate_topic_name("a/+"), Err(TopicNameError::Wildcard));
        assert_eq!(validate_topic_name("a/#"), Err(TopicNameError::Wildcard));
        assert_eq!(validate_topic_name("a/\0/b"), Err(TopicNameError::NulChar));
    }

    #[test]
    fn test_unsubscribe_invalid_filter_does_not_remove_valid_subscription() {
        let mut trie = TopicTrie::new();
        trie.subscribe("a/#", "c1".into(), QoS::AtMostOnce);
        trie.unsubscribe("a/#/b", "c1");
        assert_eq!(
            trie.match_topic("a/x").len(),
            1,
            "unsubscribe con filtro invalido 'a/#/b' non deve rimuovere 'a/#'"
        );

        trie.subscribe("#", "c2".into(), QoS::AtMostOnce);
        trie.unsubscribe("#/x", "c2");
        let matched_c2 = trie
            .match_topic("any/topic")
            .into_iter()
            .any(|s| s.client_id == "c2");
        assert!(
            matched_c2,
            "unsubscribe con filtro invalido '#/x' non deve rimuovere '#'"
        );
    }
}
