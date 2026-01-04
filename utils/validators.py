"""
File: utils/validators.py
Location: telegram_scheduler_bot/utils/validators.py
Purpose: Input validation utilities
Reusable: YES - Copy for any project needing number range parsing
"""

def parse_number_range(text: str):
    """
    Parse number or range string to list of numbers
    
    Args:
        text: Number range string
    
    Returns:
        list: List of integers
    
    Supports:
        - Single number: "5" → [5]
        - Range: "5-10" → [5, 6, 7, 8, 9, 10]
        - Multiple: "1,3,5" → [1, 3, 5]
        - Mixed: "1,3,5-8,10" → [1, 3, 5, 6, 7, 8, 10]
    
    Examples:
        parse_number_range("5") → [5]
        parse_number_range("5-10") → [5, 6, 7, 8, 9, 10]
        parse_number_range("1,3,5") → [1, 3, 5]
        parse_number_range("1-3,7,9-11") → [1, 2, 3, 7, 9, 10, 11]
    
    Used for:
        - /deletechannel 5-10
        - /deletepost 1,3,5
        - /movepost 10-20 tomorrow
    """
    numbers = []
    
    for part in text.split(','):
        part = part.strip()
        
        if '-' in part:
            # Range: 5-10
            try:
                start, end = part.split('-')
                start_num = int(start.strip())
                end_num = int(end.strip())
                
                if start_num > end_num:
                    raise ValueError(f"Invalid range: {part} (start > end)")
                
                numbers.extend(range(start_num, end_num + 1))
            except ValueError as e:
                raise ValueError(f"Invalid range format: {part}") from e
        else:
            # Single number: 5
            try:
                numbers.append(int(part))
            except ValueError:
                raise ValueError(f"Invalid number: {part}")
    
    # Remove duplicates and sort
    return sorted(list(set(numbers)))