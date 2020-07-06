package com.ifchange.sparkstreaming.v1.util;

public class TextSimilaryUtil2 {
	private static int compare(String str, String target) {
		int d[][]; // 矩阵
		int n = str.length();
		int m = target.length();
		int i; // 遍历str的
		int j; // 遍历target的
		char ch1; // str的
		char ch2; // target的
		int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1

		if (n == 0) {
			return m;
		}

		if (m == 0) {
			return n;
		}

		d = new int[n + 1][m + 1];

		for (i = 0; i <= n; i++) { // 初始化第一列
			d[i][0] = i;
		}

		for (j = 0; j <= m; j++) { // 初始化第一行
			d[0][j] = j;
		}

		for (i = 1; i <= n; i++) { // 遍历str
			ch1 = str.charAt(i - 1);
			// 去匹配target
			for (j = 1; j <= m; j++) {
				ch2 = target.charAt(j - 1);
				if (ch1 == ch2) {
					temp = 0;
				} else {
					temp = 1;
				}

				// 左边+1,上边+1, 左上角+temp取最小
				d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1]
						+ temp);
			}
		}

		return d[n][m];
	}

	private static int min(int one, int two, int three) {
		return (one = one < two ? one : two) < three ? one : three;
	}

	/**
	 * 获取两字符串的相似度
	 * 
	 * @param str
	 * @param target
	 * 
	 * @return
	 */

	public static float getSimilarityRatio(String str, String target) {
		return 1 - (float) compare(str, target)
				/ Math.max(str.length(), target.length());

	}
	
	public static void main(String[] args) {
//        String str = "意法半导体制造(深圳)有限公司 设备维护技师\n * 负责DPAK测试部门所有技术员对handle维护维修知识的工作培训,同时对技术员的能力进行考核。\n* 对设备故障进行分析,维修,同时制定预防性维护计划并执行。 ; \n * 对新设备进行安装调试及验收。 ; \n * 对设备进行合理化改造以提高生产效率,节约成本。 ; \n * 负责laser设备的维护和维修。\n\n质量方面:\n * 负责设备的调试和参数的优化,改善产品的质量,成品率 ; \n * 负责产品外观和管脚的缺陷的预防和改善。 ; \n * 参与提高产品良品率,配合工艺工程师和质量部门解决生产过程中的质量问题:成品率过低,我们对不良产品进行细致的分析。 ; \n * 定期培训所有技术人员对不良产品的了解和预防。\n\n优势与特长:\n * 在外资企业有达5年的工作经历,认同国外公司文化与价值观 ; \n * 熟悉各种测试设备具有丰富的实践经验。 ; \n * 具备独立的工作能力以及团队精神。 ; \n * 积极端正的工作和生活态度,渴望自我挑战、学习与提高。\n\n个人荣誉:\n * 荣获黄冈职业技术学院特别奖学金,三好学生 ; \n * 获国家励志奖学金 ; \n * 参与机械手运动部分改进,荣获ST全球金奖 ;";
//        String target = "意法半导体制造(深圳)有限公司 * 倒班技术员,主要负责设备稳定运行,保证产量最大化和质量零缺陷。\n* 与领班做好沟通合理安排工作重点,培训操作员正确操作设备。 ; \n * 与工程师团队合作,调试设备。";
//        System.out.println("similarityRatio=" + TextSimilaryUtil2.getSimilarityRatio(str, target));
		  float a1=3;
		  float a=(float) (1.0/3);
		  float ss=(float) (0.121312/a);
		  
		  System.out.println(ss);
		
    }
}
