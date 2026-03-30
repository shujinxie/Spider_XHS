import os
from datetime import datetime, date
from loguru import logger

from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init, load_mysql_config
from xhs_utils.data_util import handle_note_info, handle_comment_info, download_note, save_to_xlsx
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

        proxies=None,
    ):
        """
        按多个关键词搜索（OR），按时间范围过滤后按热度（点赞数）排序。
        save_choice: mysql / excel / all
        """
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
        lowered_keywords = [k.strip().lower() for k in keywords if k.strip()]


        for note_url in dedup_urls:
            success, msg, note_info = self.spider_note(note_url, cookies_str, proxies)
            if not success or not note_info:
                continue

            text_body = f"{note_info.get('title', '')} {note_info.get('desc', '')}".lower()
            keyword_hits = [k for k in lowered_keywords if k in text_body]
            if not keyword_hits:
                continue

            upload_day = self._parse_upload_date(note_info['upload_time'])
            if not (start_day <= upload_day <= end_day):
                continue

            note_info['keyword_hits'] = keyword_hits
            note_list.append(note_info)


                    comment_list.extend(comments)
                else:
                    logger.warning(f"评论抓取失败 {note_url}: {c_msg}")


        if len(note_list) > max_result_num:
            note_list = note_list[:max_result_num]
            note_ids = {n['note_id'] for n in note_list}
            comment_list = [c for c in comment_list if c['note_id'] in note_ids]


        if not note_list:
            logger.warning('未抓取到符合关键词与时间范围的笔记，跳过导出和入库。')
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


    data_spider.spider_topic_notes(
        keywords=keywords,
        start_date=start_date,
        end_date=end_date,
        cookies_str=cookies_str,
        base_path=base_path,
        per_keyword_search_num=80,
        max_result_num=100,
        include_comments=True,

        proxies=None,
    )
