import os
from datetime import datetime, date
from loguru import logger

from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init, load_mysql_config
from xhs_utils.data_util import handle_note_info, handle_comment_info, download_note, save_to_xlsx, append_to_xlsx
from xhs_utils.mysql_util import save_notes_and_comments_to_mysql


class Data_Spider:
    def __init__(self):
        self.xhs_apis = XHS_Apis()

    def spider_note(self, note_url: str, cookies_str: str, proxies=None):
        note_info = None
        try:
            success, msg, note_info = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
            if success:
                note_info = note_info['data']['items'][0]
                note_info['url'] = note_url
                note_info = handle_note_info(note_info)
        except Exception as e:
            success = False
            msg = e
        logger.info(f'爬取笔记信息 {note_url}: {success}, msg: {msg}')
        return success, msg, note_info

    def spider_note_comments(self, note_url: str, cookies_str: str, proxies=None):
        comments = []
        try:
            success, msg, raw_comments = self.xhs_apis.get_note_all_comment(note_url, cookies_str, proxies)
            if not success:
                return success, msg, comments

            for out_comment in raw_comments:
                out_comment['note_url'] = note_url
                comments.append(handle_comment_info(out_comment))
                for inner_comment in out_comment.get('sub_comments', []):
                    inner_comment['note_id'] = out_comment['note_id']
                    inner_comment['note_url'] = note_url
                    comments.append(handle_comment_info(inner_comment))
        except Exception as e:
            success = False
            msg = e
        logger.info(f'爬取评论 {note_url}: {success}, msg: {msg}, 评论数: {len(comments)}')
        return success, msg, comments

    @staticmethod
    def _parse_upload_date(upload_time: str):
        return datetime.strptime(upload_time, '%Y-%m-%d %H:%M:%S').date()

    @staticmethod
    def _date_label(start_date: date, end_date: date):
        if start_date == end_date:
            return start_date.strftime('%Y-%m-%d')
        if start_date.year == end_date.year and start_date.month == end_date.month:
            return start_date.strftime('%Y-%m')
        return f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"

    @staticmethod
    def _heat_to_int(value):
        """
        小红书热度字段可能是 1234 / '1234' / '1.3万' / '2亿' / None
        统一转换为 int，异常时返回 0。
        """
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().lower()
        if text == '':
            return 0
        try:
            if text.endswith('万'):
                return int(float(text[:-1]) * 10000)
            if text.endswith('亿'):
                return int(float(text[:-1]) * 100000000)
            return int(float(text.replace(',', '')) )
        except Exception:
            return 0

    def spider_topic_notes(
        self,
        keywords: list,
        start_date: str,
        end_date: str,
        cookies_str: str,
        base_path: dict,
        per_keyword_search_num: int = 80,
        max_result_num: int = 100,
        include_comments: bool = True,
        save_choice: str = 'mysql',
        mysql_config: dict = None,
        excel_prefix: str = '通胀文本xhs',
        proxies=None,
    ):
        """
        按多个关键词搜索（OR），按时间范围过滤后按热度（点赞数）排序。
        save_choice: mysql / excel / all / media
        """
        save_choice = save_choice.lower().strip()
        valid_choices = {'mysql', 'excel', 'all', 'media'}
        if save_choice not in valid_choices:
            raise ValueError(f"save_choice 必须是 {valid_choices} 之一，当前是: {save_choice}")

        start_day = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_day = datetime.strptime(end_date, '%Y-%m-%d').date()
        if end_day < start_day:
            raise ValueError('end_date 不能早于 start_date')

        candidate_urls = []
        failed_msgs = []
        for keyword in keywords:
            success, msg, notes = self.xhs_apis.search_some_note(
                query=keyword,
                require_num=per_keyword_search_num,
                cookies_str=cookies_str,
                sort_type_choice=2,
                note_type=0,
                note_time=0,
                note_range=0,
                pos_distance=0,
                geo=None,
                proxies=proxies,
            )
            if not success:
                failed_msgs.append(str(msg))
                logger.warning(f"关键词 {keyword} 搜索失败: {msg}")
                continue
            note_items = [n for n in notes if n.get('model_type') == 'note']
            for note in note_items:
                candidate_urls.append(f"https://www.xiaohongshu.com/explore/{note['id']}?xsec_token={note['xsec_token']}")


        if not candidate_urls and failed_msgs and all('crypto-js' in m for m in failed_msgs):
            raise RuntimeError(
                "Node 依赖缺失：检测到 crypto-js 模块不存在。请先在项目根目录执行 npm install，再运行 python main.py"
            )

        dedup_urls = list(dict.fromkeys(candidate_urls))
        logger.info(f'候选笔记数量（去重后）: {len(dedup_urls)}')

        note_list = []
        comment_list = []
        fetched_success_count = 0
        out_of_range_count = 0
        lowered_keywords = [k.strip().lower() for k in keywords if k.strip()]

        # Excel 文件命名：通胀文本xhs_开始日期
        excel_name = f'{excel_prefix}_{start_date}'
        excel_file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}.xlsx'))
        excel_comment_file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}_评论.xlsx'))

        # 每次运行覆盖旧文件，避免重复追加历史数据
        if save_choice in ['excel', 'all']:
            for p in [excel_file_path, excel_comment_file_path]:
                if os.path.exists(p):
                    os.remove(p)

        for note_url in dedup_urls:
            success, msg, note_info = self.spider_note(note_url, cookies_str, proxies)
            if not success or not note_info:
                continue
            fetched_success_count += 1

            text_body = f"{note_info.get('title', '')} {note_info.get('desc', '')}".lower()
            keyword_hits = [k for k in lowered_keywords if k in text_body]
            # 搜索结果已经按关键词召回，这里不再因为标题/描述未直接命中而丢弃
            if not keyword_hits:
                keyword_hits = ['search_result_match']

            upload_day = self._parse_upload_date(note_info['upload_time'])
            if not (start_day <= upload_day <= end_day):
                out_of_range_count += 1
                continue

            note_info['keyword_hits'] = keyword_hits
            note_list.append(note_info)

            if save_choice in ['excel', 'all']:
                append_to_xlsx(note_info, excel_file_path, type='note')

            current_comments = []
            if include_comments:
                c_success, c_msg, comments = self.spider_note_comments(note_url, cookies_str, proxies)
                if c_success:
                    current_comments = comments
                    comment_list.extend(comments)
                    if save_choice in ['excel', 'all']:
                        for comment in comments:
                            append_to_xlsx(comment, excel_comment_file_path, type='comment')
                else:
                    logger.warning(f"评论抓取失败 {note_url}: {c_msg}")

            # 边爬边写 MySQL，避免中途报错导致数据库没有数据
            if save_choice in ['mysql', 'all']:
                if not mysql_config:
                    raise ValueError('save_choice 为 mysql/all 时，mysql_config 不能为空')
                save_notes_and_comments_to_mysql([note_info], current_comments, mysql_config)

        note_list.sort(key=lambda n: self._heat_to_int(n.get('liked_count', 0)), reverse=True)

        if save_choice in ['excel', 'all'] and note_list:
            top500_notes = note_list[:500]
            top500_file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}_前500.xlsx'))
            save_to_xlsx(top500_notes, top500_file_path, type='note')

        if save_choice not in ['excel', 'all'] and len(note_list) > max_result_num:
            note_list = note_list[:max_result_num]
            note_ids = {n['note_id'] for n in note_list}
            comment_list = [c for c in comment_list if c['note_id'] in note_ids]

        if not note_list:
            logger.warning(f'未抓取到符合关键词与时间范围的笔记，跳过导出和入库。抓取成功 {fetched_success_count} 条，其中超出时间范围 {out_of_range_count} 条。')
            return note_list, comment_list

        if save_choice in ['excel', 'all']:
            file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}.xlsx'))
            save_to_xlsx(note_list, file_path, type='note')
            if include_comments and comment_list:
                comment_file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}_评论.xlsx'))
                save_to_xlsx(comment_list, comment_file_path, type='comment')

        if save_choice in ['media', 'all']:
            for note_info in note_list:
                download_note(note_info, base_path['media'], save_choice)

        logger.info(f'最终入库/导出笔记数: {len(note_list)}, 评论数: {len(comment_list)}')
        return note_list, comment_list


if __name__ == '__main__':
    cookies_str, base_path = init()
    mysql_config = load_mysql_config()
    data_spider = Data_Spider()

    # 通胀预期相关关键词（满足任意一个即可）
    keywords = [
        '通胀预期', 'CPI', 'PPI', '通货膨胀', '美国通胀', '核心CPI',
        '降息预期', '加息预期', '美联储', '实际利率', '通缩预期'
    ]

    # 时间上下界：支持按天/按月（按月可传该月 1 日到该月最后一日）
    start_date = '2026-03-01'
    end_date = '2026-03-31'

    storage_backend = 'mysql'  # mysql / excel / all / media

    data_spider.spider_topic_notes(
        keywords=keywords,
        start_date=start_date,
        end_date=end_date,
        cookies_str=cookies_str,
        base_path=base_path,
        per_keyword_search_num=80,
        max_result_num=100,
        include_comments=True,
        save_choice=storage_backend,
        mysql_config=mysql_config,
        excel_prefix='通胀文本xhs',
        proxies=None,
    )
