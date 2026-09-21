use ferrite_protocol::QoS;
use std::collections::{HashMap, HashSet};

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

    /// Inserisce un filtro di sottoscrizione (es. "sensors/+/temperature") per un dato client.
    pub fn subscribe(&mut self, topic_filter: &str, client_id: String, qos: QoS) {
        let tokens: Vec<&str> = topic_filter.split('/').collect();
        let mut current = &mut self.root;

        for (idx, &token) in tokens.iter().enumerate() {
            if token == "#" {
                // '#' deve essere l'ultimo carattere del filtro
                current.hash_subscribers.insert(client_id, qos);
                return;
            } else if token == "+" {
                if current.plus_child.is_none() {
                    current.plus_child = Some(Box::new(TrieNode::default()));
                }
                current = current.plus_child.as_deref_mut().unwrap();
            } else {
                current = current
                    .children
                    .entry(token.to_string())
                    .or_insert_with(TrieNode::default);
            }

            // Se siamo all'ultimo livello e non era '#', registriamo la sottoscrizione
            if idx == tokens.len() - 1 {
                current.subscribers.insert(client_id.clone(), qos);
            }
        }
    }
    /// Rimuove la sottoscrizione di un client a uno specifico topic filter.
    pub fn unsubscribe(&mut self, topic_filter: &str, client_id: &str) {
        let tokens: Vec<&str> = topic_filter.split('/').collect();
        Self::remove_recursive(&mut self.root, &tokens, 0, client_id);
    }

    fn remove_recursive(node: &mut TrieNode, tokens: &[&str], idx: usize, client_id: &str) -> bool {
        if idx < tokens.len() {
            let token = tokens[idx];
            if token == "#" {
                node.hash_subscribers.remove(client_id);
            } else if token == "+" {
                if let Some(plus) = node.plus_child.as_deref_mut() {
                    Self::remove_recursive(plus, tokens, idx + 1, client_id);
                }
            } else if let Some(child) = node.children.get_mut(token) {
                Self::remove_recursive(child, tokens, idx + 1, client_id);
            }
        } else {
            node.subscribers.remove(client_id);
        }

        // Cleanup: i rami vuoti possono essere potati (ottimizzazione futura)
        node.subscribers.is_empty()
            && node.children.is_empty()
            && node.plus_child.is_none()
            && node.hash_subscribers.is_empty()
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
        // 1. Chiunque sia sottoscritto con '#' a questo livello matcha tutto ciò che segue
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
        if let Some(plus) = node.plus_child.as_deref() {
            Self::match_recursive(plus, tokens, idx + 1, results);
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
}