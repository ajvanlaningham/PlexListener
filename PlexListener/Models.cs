using System.Collections.Generic;

namespace PlexListener
{
    public class FileNode
    {
        public string Name { get; set; } // File name
        public long Size { get; set; }   // File size in bytes
    }

    public class FolderNode
    {
        public string Name { get; set; } // Folder name
        public List<FileNode> Files { get; set; } = new List<FileNode>(); // Files in this folder
        public List<FolderNode> Subfolders { get; set; } = new List<FolderNode>(); // Subfolders
    }
}
