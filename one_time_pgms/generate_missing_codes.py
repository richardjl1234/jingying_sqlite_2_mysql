#!/usr/bin/env python3
"""
Generate pinyin codes for missing values and export to Excel.
This is a one-time program to help fix missing codes in 定额型号类别编码_updated.xlsx
"""

import pandas as pd
from datetime import datetime
import re


def remove_punctuation(text):
    """
    Remove all punctuation from text (Chinese and English).
    """
    if not isinstance(text, str):
        return text
    # Remove Chinese punctuation
    chinese_punctuation = '！？｡。＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、丨』…「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—''‛""„‟…‧﹏'
    # Remove English punctuation
    english_punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    all_punctuation = chinese_punctuation + english_punctuation
    
    result = text
    for char in all_punctuation:
        result = result.replace(char, '')
    return result


def generate_pinyin_code(text):
    """
    Generate pinyin-based code from Chinese text.
    Uses pypinyin to convert Chinese characters to pinyin.
    """
    try:
        from pypinyin import lazy_pinyin
        
        # Remove punctuation first
        clean_text = remove_punctuation(text)
        
        # Get pinyin
        pinyin_list = lazy_pinyin(clean_text)
        
        # Extract first letter of each pinyin
        code = ''.join([p[0] if p else '' for p in pinyin_list])
        
        return code.upper()  # Convert to uppercase
    except ImportError:
        # Fallback: if pypinyin not available, return placeholder
        return "PX"


def generate_codes_for_missing_values():
    """
    Generate pinyin codes for the missing values and export to Excel.
    """
    # Missing values from the error
    missing_cat2 = []
    missing_process = [
        'IE3合计', 'IE3套壳', '合计金额', '定子绕嵌排线合计', 
        '打孔攻丝:IE3QL', '整机喷漆转子合计', '磨轴外圆合计IE3自动', 
        '转子校平衡:IE3', '转子热套IE3', '转子车外圆:数控IE3', 
        '轴转子磨铣车普通机床手动合计', '轴转子磨铣车自动机床合计'
    ]
    
    print("Generating pinyin codes for missing values...")
    print("=" * 60)
    
    # Generate codes for category 2
    cat2_data = []
    print("\n=== Category 2 (类别2) ===")
    for name in missing_cat2:
        code = generate_pinyin_code(name)
        cat2_data.append({
            '类别2编码': code,
            '类别2': name
        })
        print(f"  {name} -> {code}")
    
    # Generate codes for process
    process_data = []
    print("\n=== Process (加工工序) ===")
    for name in missing_process:
        code = generate_pinyin_code(name)
        process_data.append({
            '加工工序编码': code,
            '加工工序': name
        })
        print(f"  {name} -> {code}")
    
    # Create DataFrames
    df_cat2 = pd.DataFrame(cat2_data)
    df_process = pd.DataFrame(process_data)
    
    # Output to Excel
    output_file = "missing_codes_output.xlsx"
    
    print(f"\n{'=' * 60}")
    print(f"Writing to Excel file: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df_cat2.to_excel(writer, sheet_name='类别2', index=False)
        df_process.to_excel(writer, sheet_name='加工工序', index=False)
    
    print(f"\nExcel file created successfully!")
    print(f"\nNext steps:")
    print(f"1. Open {output_file}")
    print(f"2. Copy the codes to 定额型号类别编码_updated.xlsx")
    print(f"   - Add rows to '类别2' sheet")
    print(f"   - Add rows to '加工工序' sheet")
    print(f"3. Re-run the pipeline")
    
    return df_cat2, df_process


if __name__ == "__main__":
    generate_codes_for_missing_values()
