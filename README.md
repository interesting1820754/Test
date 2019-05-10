上传文件
git init
git add 文件（或文件夹）名
git commit -m "text" 　　　　　　　//""号内的内容为注释
git remote add origin https://github.com/interesting1820754/Test.git
git push -u origin master

出现此错误
error: 无法推送一些引用到 'https://github.com/interesting1820754/Test'
提示：更新被拒绝，因为远程仓库包含您本地尚不存在的提交。这通常是因为另外
提示：一个仓库已向该引用进行了推送。再次推送前，您可能需要先整合远程变更
提示：（如 'git pull ...'）。
提示：详见 'git push --help' 中的 'Note about fast-forwards' 小节。
输入

git pull origin master

删除文件
git rm -r --cached 文件（或文件夹）名　　　　　　　//若是文件可以将-r去掉

git commit -m 'delete'

git push -u origin master


Linux下 保存 git账号密码
https://www.cnblogs.com/boystar/p/5644025.html
