"""
{
    'raw_id': '677923255b460df5b5cb3c78', # raw data id in mongo atlas
    'message_id': 1848,
    'channel_id': 1293830821,
    'date': '2024-11-19T08:34:55+00:00',
    'message': 'lorem ipsum',
    'forwards': 50,
    'views': 5617,
    'reactions': [('👍', 15), ('❤', 3)],
}



@dataclass
class Product:
    # id: str = field(default_factory=lambda: str(ObjectId())) 
    # name: Optional[str] = None
    description: Optional[str] = None
    # summary: Optional[str] = None
    # price: Optional[float] = None
    categories: list[Category] = field(default_factory=list)
    # is_available: Optional[bool] = True
    # images: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    channel_id: str
    posted_at: Optional[datetime] = None




"""
from storage.store import fetch_stored_messages, store_raw_data, store_latest_and_oldest_ids, store_products
from tqdm import tqdm


def preprocess():
    stored_messages = fetch_stored_messages(collection_name="raw_data")
    preprocessed = []
    for raw_message in stored_messages:
        filtered_data = extract_message_data(raw_message)
        if filtered_data:
            preprocessed.append(filtered_data)
    
    print('Storing preprocessed data...')
    stored = store_raw_data(preprocessed, collection_name="stage1_preprocessed_data")
    print('Success!' if stored else 'Fail!')

def extract_oldest_and_latest_ids():
    stored_messages = fetch_stored_messages(collection_name="raw_data")
    oldest_id = {}
    latest_id = {}

    for raw_message in tqdm(stored_messages, desc="Processing raw messages"):
        filted_message = extract_message_data(raw_message)
        if not filted_message:
            continue
        message_id = filted_message.get('message_id')
        channel_id = filted_message.get('channel_id')
        
        if channel_id not in oldest_id:
            oldest_id[channel_id] = message_id
            latest_id[channel_id] = message_id
        else:
            oldest_id[channel_id] = min(oldest_id[channel_id], message_id)
            latest_id[channel_id] = max(latest_id[channel_id], message_id)

    storage = store_latest_and_oldest_ids(latest_id, oldest_id)
    if not storage:
        print('Oldest-Latest ids storage failed!')
    else:
        print('Oldest-Latest ids stored successfully!')
            
        

def create_channel(channel_data):
    pass

def create_product(product_data):
    pass

def extract_message_data(message_obj):
    """
    Extracts specific fields from a message object into a simplified dictionary.
    
    Parameters:
        message_obj (dict): The raw message object.

    Returns:
        dict: A dictionary containing the extracted fields.
    """
    try:
        # Get raw ID and message ID
        raw_id = message_obj.get('_id')
        message_id = message_obj.get('id')
        
        # Get channel ID
        peer_id = message_obj.get('peer_id', {})
        channel_id = peer_id.get('channel_id') if peer_id.get('_') == 'PeerChannel' else None

        # Extract additional fields
        date = message_obj.get('date')
        message = message_obj.get('message', '')
        forwards = message_obj.get('forwards', 0)
        views = message_obj.get('views', 0)

        if not message:
            return None

        # Extract reactions
        reactions_data = []
        reactions = message_obj.get('reactions', {})
        if reactions:
            results = reactions.get('results', [])
            for reaction in results:
                emoji = reaction.get('reaction', {}).get('emoticon', '')
                count = reaction.get('count', 0)
                if emoji:
                    reactions_data.append((emoji, count))

        return {
            'raw_id': raw_id,
            'message_id': message_id,
            'channel_id': channel_id,
            'date': date,
            'message': message,
            'forwards': forwards,
            'views': views,
            'reactions': reactions_data,
        }
    except:
        return None


def insert_into_products_collection():
    print('Fetching from database... stage 1 preprocessed data')
    stored_messages = fetch_stored_messages(collection_name="stage1_preprocessed_data")
    products = []
    seen_messages = set()
    for message in tqdm(stored_messages, desc="Processing messages into Products"):
        if message['message_id'] in seen_messages:
            continue
        seen_messages.add(message['message_id'])
        message.pop('raw_id')
        
        date = message.pop('date')
        message['posted_at'] = date
        
        desc = message.pop('message')
        message['description'] = desc
        products.append(message)
        
    stored = store_products(products)
    print('Failed to store products!' if not stored else f'{len(products)} products stored successfully!' )
